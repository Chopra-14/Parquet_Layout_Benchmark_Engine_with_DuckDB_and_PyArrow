import argparse
import duckdb
import json
import os
import time
import psutil
import csv
from glob import glob

def get_peak_rss_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def find_parquet_scan(node):
    if node['name'] == 'PARQUET_SCAN' or node['name'] == 'PROJECTION' and 'PARQUET' in str(node.get('extra_info', '')):
        return node
    for child in node.get('children', []):
        result = find_parquet_scan(child)
        if result:
            return result
    return None

def extract_metrics(profile_path):
    with open(profile_path, 'r') as f:
        profile = json.load(f)
        
    scan_node = find_parquet_scan(profile)
    bytes_read = 0
    row_groups_total = 0
    row_groups_pruned = 0
    
    if scan_node and 'extra_info' in scan_node:
        info = scan_node['extra_info']
        # parse info string to find metrics
        # Depending on duckdb version it could be like:
        # "Row Groups Pruned: 5", "Total Row Groups: 10", "File Size: 12345"
        for line in info.split('\n'):
            line = line.lower()
            if 'row groups pruned' in line or 'pruned row groups' in line:
                try:
                    row_groups_pruned = int(line.split(':')[-1].strip())
                except:
                    pass
            elif 'row groups' in line and 'total' in line:
                try:
                    row_groups_total = int(line.split(':')[-1].strip())
                except:
                    pass
            elif 'bytes read' in line or 'file size' in line:
                try:
                    # Sometimes duckdb outputs just "Bytes Read: 123"
                    val = line.split(':')[-1].strip()
                    if 'mb' in val:
                        bytes_read = int(float(val.replace('mb','').strip()) * 1024 * 1024)
                    else:
                        bytes_read = int(val)
                except:
                    pass
    
    return bytes_read, row_groups_total, row_groups_pruned

def execute_and_profile(con, query: str, profile_file: str):
    con.execute("PRAGMA enable_profiling='json'")
    con.execute(f"PRAGMA profile_output='{profile_file}'")
    start = time.time()
    con.execute(query).fetchall()
    latency_ms = (time.time() - start) * 1000
    
    bytes_read, row_groups_total, row_groups_pruned = extract_metrics(profile_file)
    return latency_ms, bytes_read, row_groups_total, row_groups_pruned

def run_suite(input_dir: str, output_csv: str):
    layouts = [d for d in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, d))]
    
    # We will pick dynamic values based on data
    # Assuming start is 90 days ago, pick a range in the middle
    # But just for the query, we can use arbitrary bounds that match data
    # E.g. event_ts between '2026-08-01' and '2026-08-02' (or just use relative to now to be safe)
    
    queries = {
        'point_lookup': "SELECT * FROM '{}/**/*.parquet' WHERE user_id = 500000",
        'range_filter': "SELECT COUNT(*) FROM '{}/**/*.parquet' WHERE event_ts >= current_date() - interval '45 days' AND event_ts < current_date() - interval '44 days'",
        'medium_selectivity': "SELECT AVG(value) FROM '{}/**/*.parquet' WHERE country = 'US'",
        'full_aggregation': "SELECT event_type, COUNT(*) FROM '{}/**/*.parquet' GROUP BY event_type",
        'complex': "SELECT user_id, SUM(value) FROM '{}/**/*.parquet' WHERE event_ts >= current_date() - interval '45 days' AND event_ts < current_date() - interval '44 days' GROUP BY user_id ORDER BY SUM(value) DESC LIMIT 10"
    }

    results = []
    
    # Global SUM validation
    global_sum = None
    
    for layout in layouts:
        layout_path = os.path.join(input_dir, layout)
        
        # skip if no parquet files
        if not glob(f"{layout_path}/**/*.parquet", recursive=True):
            continue
            
        con = duckdb.connect()
        try:
            current_sum = con.execute(f"SELECT SUM(value) FROM '{layout_path}/**/*.parquet'").fetchone()[0]
            if global_sum is None:
                global_sum = current_sum
            elif abs(global_sum - current_sum) > 0.1:
                print(f"WARNING: Checksum mismatch for layout {layout}. Expected {global_sum}, got {current_sum}")
        except Exception as e:
            print(f"Error checking sum for {layout}: {e}")
        con.close()

        # Gather metadata
        files = glob(f"{layout_path}/**/*.parquet", recursive=True)
        num_files = len(files)
        total_bytes = sum(os.path.getsize(f) for f in files) if files else 0
        
        row_group_rows = 122880 # default
        if 'rg_10k' in layout:
            row_group_rows = 10000
        elif 'rg_1m' in layout:
            row_group_rows = 1000000
            
        for q_name, q_tpl in queries.items():
            query = q_tpl.format(layout_path)
            
            # Cold runs
            for _ in range(5):
                con = duckdb.connect()
                latency, br, rgt, rgp = execute_and_profile(con, query, 'profile.json')
                peak_rss = get_peak_rss_mb()
                results.append({
                    'layout': layout, 'row_group_rows': row_group_rows, 'files': num_files,
                    'total_bytes': total_bytes, 'query': q_name, 'cache': 'cold',
                    'latency_ms': latency, 'bytes_read': br, 'row_groups_total': rgt,
                    'row_groups_pruned': rgp, 'peak_rss_mb': peak_rss
                })
                con.close()
                
            # Warm runs
            con = duckdb.connect()
            # warmup once
            con.execute(query).fetchall()
            for _ in range(5):
                latency, br, rgt, rgp = execute_and_profile(con, query, 'profile.json')
                peak_rss = get_peak_rss_mb()
                results.append({
                    'layout': layout, 'row_group_rows': row_group_rows, 'files': num_files,
                    'total_bytes': total_bytes, 'query': q_name, 'cache': 'warm',
                    'latency_ms': latency, 'bytes_read': br, 'row_groups_total': rgt,
                    'row_groups_pruned': rgp, 'peak_rss_mb': peak_rss
                })
            con.close()

    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'layout', 'row_group_rows', 'files', 'total_bytes', 'query', 'cache',
            'latency_ms', 'bytes_read', 'row_groups_total', 'row_groups_pruned', 'peak_rss_mb'
        ])
        writer.writeheader()
        writer.writerows(results)
        
    if os.path.exists('profile.json'):
        os.remove('profile.json')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--out', type=str, required=True)
    args = parser.parse_args()
    
    run_suite(args.input, args.out)

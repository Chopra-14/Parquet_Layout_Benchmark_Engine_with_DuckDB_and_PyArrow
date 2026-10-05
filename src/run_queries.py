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

def find_parquet_scan_nodes(node, results=None):
    """Recursively search for all PARQUET_SCAN nodes in a DuckDB profile tree."""
    if results is None:
        results = []
    if isinstance(node, dict):
        name = node.get('name', '')
        if 'PARQUET_SCAN' in name or 'PARQUET_SCAN' in str(node.get('extra_info', '')):
            results.append(node)
        for child in node.get('children', []):
            find_parquet_scan_nodes(child, results)
    return results

def parse_extra_info(extra_info_str):
    """Parse key-value pairs from DuckDB extra_info string."""
    bytes_read = 0
    row_groups_total = 0
    row_groups_pruned = 0
    if not extra_info_str:
        return bytes_read, row_groups_total, row_groups_pruned
    for line in str(extra_info_str).split('\n'):
        ll = line.lower().strip()
        if 'row groups pruned' in ll or 'pruned row groups' in ll:
            try:
                row_groups_pruned = int(ll.split(':')[-1].strip())
            except:
                pass
        elif 'total row groups' in ll or ('row groups' in ll and 'total' in ll):
            try:
                row_groups_total = int(ll.split(':')[-1].strip())
            except:
                pass
        elif 'file size' in ll:
            try:
                val = ll.split(':')[-1].strip()
                bytes_read = int(val)
            except:
                pass
        elif 'bytes read' in ll:
            try:
                val = ll.split(':')[-1].strip()
                bytes_read = int(val)
            except:
                pass
    return bytes_read, row_groups_total, row_groups_pruned

def extract_metrics_from_profile(profile_path):
    """Read profile JSON and extract parquet scan metrics."""
    try:
        with open(profile_path, 'r') as f:
            profile = json.load(f)
    except Exception:
        return 0, 0, 0

    scan_nodes = find_parquet_scan_nodes(profile)
    total_bytes = 0
    total_rg = 0
    pruned_rg = 0
    for node in scan_nodes:
        extra = node.get('extra_info', '')
        b, t, p = parse_extra_info(extra)
        total_bytes += b
        total_rg += t
        pruned_rg += p
    return total_bytes, total_rg, pruned_rg

def get_parquet_glob(layout_path):
    """Return a glob pattern that finds parquet files under any layout structure."""
    # Check if there are parquet files at top-level or nested
    top_level = glob(os.path.join(layout_path, '*.parquet'))
    nested = glob(os.path.join(layout_path, '**', '*.parquet'), recursive=True)
    if top_level:
        return os.path.join(layout_path, '*.parquet').replace('\\', '/')
    elif nested:
        return os.path.join(layout_path, '**', '*.parquet').replace('\\', '/')
    return None

def execute_and_profile(con, query: str, profile_file: str):
    profile_file_escaped = profile_file.replace('\\', '/')
    con.execute("PRAGMA enable_profiling='json'")
    con.execute(f"PRAGMA profile_output='{profile_file_escaped}'")
    start = time.time()
    try:
        con.execute(query).fetchall()
    except Exception as e:
        print(f"    Query error: {e}")
    latency_ms = (time.time() - start) * 1000
    bytes_read, rg_total, rg_pruned = extract_metrics_from_profile(profile_file)
    return latency_ms, bytes_read, rg_total, rg_pruned

def get_layout_info(layout_path):
    """Get file count, total bytes, and default row group rows for a layout."""
    all_files = glob(os.path.join(layout_path, '**', '*.parquet'), recursive=True)
    num_files = len(all_files)
    total_bytes = sum(os.path.getsize(f) for f in all_files) if all_files else 0
    return num_files, total_bytes

def run_suite(input_dir: str, output_csv: str):
    os.makedirs(os.path.dirname(output_csv) if os.path.dirname(output_csv) else '.', exist_ok=True)

    layouts = sorted([d for d in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, d))])
    profile_file = os.path.abspath('profile.json')

    # Build queries — use glob pattern placeholder
    queries = {
        'point_lookup':       "SELECT * FROM read_parquet('{glob}', hive_partitioning=1) WHERE user_id = 500000",
        'range_filter':       "SELECT COUNT(*) FROM read_parquet('{glob}', hive_partitioning=1) WHERE event_ts >= (current_date() - INTERVAL 45 DAY) AND event_ts < (current_date() - INTERVAL 44 DAY)",
        'medium_selectivity': "SELECT AVG(value) FROM read_parquet('{glob}', hive_partitioning=1) WHERE country = 'US'",
        'full_aggregation':   "SELECT event_type, COUNT(*) FROM read_parquet('{glob}', hive_partitioning=1) GROUP BY event_type",
        'complex':            "SELECT user_id, SUM(value) as total FROM read_parquet('{glob}', hive_partitioning=1) WHERE event_ts >= (current_date() - INTERVAL 45 DAY) AND event_ts < (current_date() - INTERVAL 44 DAY) GROUP BY user_id ORDER BY total DESC LIMIT 10",
    }

    results = []

    # Global SUM checksum validation
    print("Validating checksums across layouts...")
    global_sum = None
    for layout in layouts:
        layout_path = os.path.join(input_dir, layout)
        glob_pat = get_parquet_glob(layout_path)
        if not glob_pat:
            continue
        try:
            con = duckdb.connect()
            current_sum = con.execute(
                f"SELECT SUM(value) FROM read_parquet('{glob_pat}', hive_partitioning=1)"
            ).fetchone()[0]
            con.close()
            if global_sum is None:
                global_sum = current_sum
                print(f"  Reference SUM(value) = {global_sum:.4f} (from {layout})")
            elif abs(global_sum - current_sum) > 1.0:
                print(f"  CHECKSUM MISMATCH: {layout} SUM={current_sum:.4f} vs expected {global_sum:.4f}")
                raise SystemExit("Data integrity check failed — aborting.")
            else:
                print(f"  OK: {layout} SUM={current_sum:.4f}")
        except SystemExit:
            raise
        except Exception as e:
            print(f"  Warning: could not validate {layout}: {e}")

    print(f"\nRunning query suite across {len(layouts)} layouts...")

    for layout in layouts:
        layout_path = os.path.join(input_dir, layout)
        glob_pat = get_parquet_glob(layout_path)
        if not glob_pat:
            print(f"  Skipping {layout} — no parquet files found.")
            continue

        num_files, total_bytes = get_layout_info(layout_path)
        row_group_rows = 122880  # default
        if 'rg_10k' in layout:
            row_group_rows = 10000
        elif 'rg_1m' in layout:
            row_group_rows = 1000000

        print(f"\n  Layout: {layout} ({num_files} files, {total_bytes // 1024 // 1024} MB)")

        for q_name, q_tpl in queries.items():
            query = q_tpl.format(glob=glob_pat)
            print(f"    Query: {q_name}")

            # --- 5 Cold runs (fresh connection each time) ---
            for i in range(5):
                con = duckdb.connect()
                latency, br, rgt, rgp = execute_and_profile(con, query, profile_file)
                peak_rss = get_peak_rss_mb()
                results.append({
                    'layout': layout, 'row_group_rows': row_group_rows,
                    'files': num_files, 'total_bytes': total_bytes,
                    'query': q_name, 'cache': 'cold',
                    'latency_ms': round(latency, 2), 'bytes_read': br,
                    'row_groups_total': rgt, 'row_groups_pruned': rgp,
                    'peak_rss_mb': round(peak_rss, 2)
                })
                con.close()

            # --- 5 Warm runs (same connection) ---
            con = duckdb.connect()
            con.execute(query).fetchall()  # warmup
            for i in range(5):
                latency, br, rgt, rgp = execute_and_profile(con, query, profile_file)
                peak_rss = get_peak_rss_mb()
                results.append({
                    'layout': layout, 'row_group_rows': row_group_rows,
                    'files': num_files, 'total_bytes': total_bytes,
                    'query': q_name, 'cache': 'warm',
                    'latency_ms': round(latency, 2), 'bytes_read': br,
                    'row_groups_total': rgt, 'row_groups_pruned': rgp,
                    'peak_rss_mb': round(peak_rss, 2)
                })
            con.close()

    # Write CSV
    fieldnames = [
        'layout', 'row_group_rows', 'files', 'total_bytes', 'query', 'cache',
        'latency_ms', 'bytes_read', 'row_groups_total', 'row_groups_pruned', 'peak_rss_mb'
    ]
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    if os.path.exists(profile_file):
        os.remove(profile_file)

    print(f"\nResults written to {output_csv} ({len(results)} rows)")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--out', type=str, required=True)
    args = parser.parse_args()
    run_suite(args.input, args.out)

import argparse
import duckdb
import time
import os
import psutil
import csv
from glob import glob

def get_peak_rss_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def compact(input_dir: str, output_dir: str, stats_file: str):
    os.makedirs(output_dir, exist_ok=True)
    
    con = duckdb.connect()
    
    # Pre-compaction stats
    start_time = time.time()
    
    input_files_list = glob(f"{input_dir}/**/*.parquet", recursive=True)
    input_files = len(input_files_list)
    
    # Duckdb will read all tiny files
    # We want to compact them into daily files sorted by user_id
    query_before = f"SELECT COUNT(*), SUM(value) FROM '{input_dir}/**/*.parquet'"
    rows_before, checksum_before = con.execute(query_before).fetchone()
    
    # We can do this efficiently using duckdb COPY ... TO ... PARTITION_BY
    # But duckdb partition by requires extracting the partition column if it doesn't exist
    # If minute/date are in the file, we can use them.
    # The requirement says: output to a new folder chunked daily, sorted by user_id.
    
    compaction_query = f"""
    COPY (
        SELECT * FROM '{input_dir}/**/*.parquet' ORDER BY user_id
    ) TO '{output_dir}' (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE 1)
    """
    con.execute(compaction_query)
    
    wall_s = time.time() - start_time
    peak_rss = get_peak_rss_mb()
    
    output_files_list = glob(f"{output_dir}/**/*.parquet", recursive=True)
    output_files = len(output_files_list)
    
    query_after = f"SELECT COUNT(*), SUM(value) FROM '{output_dir}/**/*.parquet'"
    rows_after, checksum_after = con.execute(query_after).fetchone()
    
    checksum_match = (abs(checksum_before - checksum_after) < 0.1)
    
    con.close()
    
    with open(stats_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'input_files', 'output_files', 'wall_s', 'peak_rss_mb', 
            'rows_before', 'rows_after', 'checksum_match'
        ])
        writer.writeheader()
        writer.writerow({
            'input_files': input_files,
            'output_files': output_files,
            'wall_s': wall_s,
            'peak_rss_mb': peak_rss,
            'rows_before': rows_before,
            'rows_after': rows_after,
            'checksum_match': 1 if checksum_match else 0
        })

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--out', type=str, required=True)
    parser.add_argument('--stats', type=str, required=True)
    args = parser.parse_args()
    
    compact(args.input, args.out, args.stats)

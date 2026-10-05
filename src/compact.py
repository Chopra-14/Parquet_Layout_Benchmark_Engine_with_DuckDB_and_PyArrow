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

def get_parquet_glob(path):
    top = glob(os.path.join(path, '*.parquet'))
    nested = glob(os.path.join(path, '**', '*.parquet'), recursive=True)
    if top:
        return os.path.join(path, '*.parquet').replace('\\', '/')
    elif nested:
        return os.path.join(path, '**', '*.parquet').replace('\\', '/')
    return None

def compact(input_dir: str, output_dir: str, stats_file: str):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.dirname(stats_file) if os.path.dirname(stats_file) else '.', exist_ok=True)

    glob_pat = get_parquet_glob(input_dir)
    if not glob_pat:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")

    con = duckdb.connect()

    # Pre-compaction stats
    input_files_list = glob(os.path.join(input_dir, '**', '*.parquet'), recursive=True) + \
                       glob(os.path.join(input_dir, '*.parquet'))
    input_files_list = list(set(input_files_list))
    input_files = len(input_files_list)

    print(f"Compacting {input_files} files from {input_dir}...")
    row_before_q = f"SELECT COUNT(*), SUM(value) FROM read_parquet('{glob_pat}', hive_partitioning=1)"
    rows_before, checksum_before = con.execute(row_before_q).fetchone()
    print(f"  rows_before={rows_before}, SUM(value)={checksum_before:.4f}")

    start_time = time.time()

    # Read all tiny files, derive date, sort by user_id, write daily files
    con.execute(f"""
        CREATE OR REPLACE TABLE _compact_tmp AS
        SELECT *, strftime(event_ts::TIMESTAMP, '%Y-%m-%d') AS _date
        FROM read_parquet('{glob_pat}', hive_partitioning=1)
        ORDER BY _date, user_id
    """)

    dates = [r[0] for r in con.execute("SELECT DISTINCT _date FROM _compact_tmp ORDER BY _date").fetchall()]
    output_files = 0
    for d in dates:
        day_path = os.path.join(output_dir, f"{d}.parquet").replace('\\', '/')
        con.execute(f"""
            COPY (
                SELECT event_ts, user_id, country, event_type, device, value
                FROM _compact_tmp WHERE _date = '{d}'
            ) TO '{day_path}' (FORMAT PARQUET, CODEC 'SNAPPY')
        """)
        output_files += 1

    wall_s = time.time() - start_time
    peak_rss = get_peak_rss_mb()

    out_glob = os.path.join(output_dir, '*.parquet').replace('\\', '/')
    rows_after, checksum_after = con.execute(
        f"SELECT COUNT(*), SUM(value) FROM read_parquet('{out_glob}')"
    ).fetchone()
    print(f"  rows_after={rows_after}, SUM(value)={checksum_after:.4f}")
    print(f"  Wall time: {wall_s:.2f}s, Peak RSS: {peak_rss:.1f} MB")

    checksum_match = 1 if abs(checksum_before - checksum_after) < 1.0 else 0
    if not checksum_match:
        print(f"  WARNING: checksum mismatch! before={checksum_before:.4f} after={checksum_after:.4f}")

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
            'wall_s': round(wall_s, 3),
            'peak_rss_mb': round(peak_rss, 2),
            'rows_before': rows_before,
            'rows_after': rows_after,
            'checksum_match': checksum_match
        })

    print(f"Compaction complete. Results: {stats_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--out', type=str, required=True)
    parser.add_argument('--stats', type=str, required=True)
    args = parser.parse_args()
    compact(args.input, args.out, args.stats)

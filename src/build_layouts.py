import argparse
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import os
import shutil

def write_partitioned_files(df, out_dir, partition_col, sort_by=None, row_group_size=None):
    """Group df by partition_col and write one Parquet file per group."""
    os.makedirs(out_dir, exist_ok=True)
    for key, group in df.groupby(partition_col):
        if sort_by:
            group = group.sort_values(by=sort_by)
        # Remove helper columns before writing
        cols_to_drop = [c for c in ['date', 'hour', 'minute'] if c in group.columns and c != partition_col]
        group = group.drop(columns=cols_to_drop)
        safe_key = str(key).replace(':', '-').replace(' ', '_')
        out_path = os.path.join(out_dir, f"{safe_key}.parquet")
        tbl = pa.Table.from_pandas(group, preserve_index=False)
        kwargs = {}
        if row_group_size:
            kwargs['row_group_size'] = row_group_size
        pq.write_table(tbl, out_path, **kwargs)

def write_hive_partitioned(df, out_dir, partition_cols, sort_by=None, row_group_size=None):
    """Write Hive-style partitioned dataset (date=YYYY-MM-DD/...)"""
    os.makedirs(out_dir, exist_ok=True)
    for keys, group in df.groupby(partition_cols):
        if not isinstance(keys, tuple):
            keys = (keys,)
        part_path = out_dir
        for col, val in zip(partition_cols, keys):
            part_path = os.path.join(part_path, f"{col}={val}")
        os.makedirs(part_path, exist_ok=True)
        if sort_by:
            group = group.sort_values(by=sort_by)
        # Drop partition columns from the file (Hive convention)
        cols_to_drop = [c for c in partition_cols if c in group.columns]
        # Also drop helper cols
        helper_cols = [c for c in ['date', 'hour', 'minute'] if c in group.columns and c not in partition_cols]
        group = group.drop(columns=cols_to_drop + helper_cols)
        tbl = pa.Table.from_pandas(group, preserve_index=False)
        kwargs = {}
        if row_group_size:
            kwargs['row_group_size'] = row_group_size
        pq.write_table(tbl, os.path.join(part_path, 'part-0.parquet'), **kwargs)

def build_all_layouts(input_file: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    print("Reading raw data...")
    df = pq.read_table(input_file).to_pandas()

    # Derive partition keys
    df['date'] = df['event_ts'].dt.strftime('%Y-%m-%d')
    df['hour'] = df['event_ts'].dt.strftime('%Y-%m-%d-%H')
    df['minute'] = df['event_ts'].dt.strftime('%Y-%m-%d-%H-%M')

    def clean_and_make(path):
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)

    # ---- 1. tiny_files: 1 file per minute ----
    print("Building tiny_files...")
    clean_and_make(os.path.join(output_dir, 'tiny_files'))
    write_partitioned_files(df, os.path.join(output_dir, 'tiny_files'), 'minute')

    # ---- 2. hourly: 1 file per hour ----
    print("Building hourly...")
    clean_and_make(os.path.join(output_dir, 'hourly'))
    write_partitioned_files(df, os.path.join(output_dir, 'hourly'), 'hour')

    # ---- 3. daily: 1 file per day, default row group size ----
    print("Building daily...")
    clean_and_make(os.path.join(output_dir, 'daily'))
    write_partitioned_files(df, os.path.join(output_dir, 'daily'), 'date')

    # ---- 4. daily_rg_10k ----
    print("Building daily_rg_10k...")
    clean_and_make(os.path.join(output_dir, 'daily_rg_10k'))
    write_partitioned_files(df, os.path.join(output_dir, 'daily_rg_10k'), 'date', row_group_size=10000)

    # ---- 5. daily_rg_1m ----
    print("Building daily_rg_1m...")
    clean_and_make(os.path.join(output_dir, 'daily_rg_1m'))
    write_partitioned_files(df, os.path.join(output_dir, 'daily_rg_1m'), 'date', row_group_size=1000000)

    # ---- 6. partitioned_by_date (Hive style: date=YYYY-MM-DD/) ----
    print("Building partitioned_by_date...")
    clean_and_make(os.path.join(output_dir, 'partitioned_by_date'))
    write_hive_partitioned(df, os.path.join(output_dir, 'partitioned_by_date'), ['date'])

    # ---- 7. partitioned_by_date_country (Hive nested) ----
    print("Building partitioned_by_date_country...")
    clean_and_make(os.path.join(output_dir, 'partitioned_by_date_country'))
    write_hive_partitioned(df, os.path.join(output_dir, 'partitioned_by_date_country'), ['date', 'country'])

    # ---- 8. sorted_by_user ----
    print("Building sorted_by_user...")
    clean_and_make(os.path.join(output_dir, 'sorted_by_user'))
    write_partitioned_files(df, os.path.join(output_dir, 'sorted_by_user'), 'date', sort_by=['user_id'])

    # ---- 9. sorted_by_ts ----
    print("Building sorted_by_ts...")
    clean_and_make(os.path.join(output_dir, 'sorted_by_ts'))
    write_partitioned_files(df, os.path.join(output_dir, 'sorted_by_ts'), 'date', sort_by=['event_ts'])

    # ---- Induced failure: partitioned_by_user_id (high cardinality anti-pattern) ----
    print("Building partitioned_by_user_id (induced failure demo)...")
    out_part_user = os.path.join(output_dir, 'partitioned_by_user_id')
    os.makedirs(out_part_user, exist_ok=True)
    try:
        # Sample a small subset to avoid blowing up disk, but still demonstrate the explosion
        sample = df.sample(min(50000, len(df)), random_state=42)
        unique_users = sample['user_id'].nunique()
        print(f"  Would create {unique_users} user_id partitions — file explosion demonstrated.")
        # Write a small sample to show the structure without crashing
        for uid, group in list(sample.groupby('user_id'))[:200]:
            uid_dir = os.path.join(out_part_user, f"user_id={uid}")
            os.makedirs(uid_dir, exist_ok=True)
            cols_to_drop = [c for c in ['date', 'hour', 'minute'] if c in group.columns]
            g = group.drop(columns=cols_to_drop)
            pq.write_table(pa.Table.from_pandas(g, preserve_index=False),
                           os.path.join(uid_dir, 'part-0.parquet'))
        print(f"  Wrote first 200 user_id partitions. Full run would produce {unique_users} dirs.")
    except Exception as e:
        print(f"  Expected failure partitioning by user_id: {e}")

    print("All layouts built successfully.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--out_dir', type=str, required=True)
    args = parser.parse_args()
    build_all_layouts(args.input, args.out_dir)

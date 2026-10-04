import argparse
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
import pandas as pd
import os
import shutil
import warnings

def build_all_layouts(input_file: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    table = pq.read_table(input_file)
    
    # Add extraction columns for partitioning/chunking
    # event_ts is timestamp('ms'). We can compute date, hour, minute.
    
    # We will use pandas for easier column derivations before writing out if needed, or pyarrow compute
    df = table.to_pandas()
    df['date'] = df['event_ts'].dt.date
    df['hour'] = df['event_ts'].dt.strftime('%Y-%m-%d-%H')
    df['minute'] = df['event_ts'].dt.strftime('%Y-%m-%d-%H-%M')
    
    enriched_table = table.append_column('date', pa.array(df['date'].astype(str)))
    enriched_table = enriched_table.append_column('hour', pa.array(df['hour']))
    enriched_table = enriched_table.append_column('minute', pa.array(df['minute']))
    
    # 1. tiny_files: minute chunking
    out_tiny = os.path.join(output_dir, 'tiny_files')
    pq.write_to_dataset(enriched_table, root_path=out_tiny, partition_cols=['minute'], existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    
    # 2. hourly
    out_hourly = os.path.join(output_dir, 'hourly')
    pq.write_to_dataset(enriched_table, root_path=out_hourly, partition_cols=['hour'], existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    
    # 3. daily (default row group size is typically 1M in pyarrow, but DuckDB is 122880, we will use default pyarrow for this or explicitly set it)
    out_daily = os.path.join(output_dir, 'daily')
    pq.write_to_dataset(enriched_table, root_path=out_daily, partition_cols=['date'], existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    
    # 4. daily_rg_10k
    out_daily_10k = os.path.join(output_dir, 'daily_rg_10k')
    pq.write_to_dataset(enriched_table, root_path=out_daily_10k, partition_cols=['date'], row_group_size=10000, existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    
    # 5. daily_rg_1m
    out_daily_1m = os.path.join(output_dir, 'daily_rg_1m')
    pq.write_to_dataset(enriched_table, root_path=out_daily_1m, partition_cols=['date'], row_group_size=1000000, existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    
    # 6. partitioned_by_date
    out_part_date = os.path.join(output_dir, 'partitioned_by_date')
    pq.write_to_dataset(enriched_table, root_path=out_part_date, partition_cols=['date'], existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    
    # 7. partitioned_by_date_country
    out_part_date_country = os.path.join(output_dir, 'partitioned_by_date_country')
    pq.write_to_dataset(enriched_table, root_path=out_part_date_country, partition_cols=['date', 'country'], existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    
    # 8. sorted_by_user
    out_sorted_user = os.path.join(output_dir, 'sorted_by_user')
    sorted_table = pa.Table.from_pandas(df.sort_values(by=['date', 'user_id']))
    pq.write_to_dataset(sorted_table, root_path=out_sorted_user, partition_cols=['date'], existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    
    # 9. sorted_by_ts
    out_sorted_ts = os.path.join(output_dir, 'sorted_by_ts')
    sorted_ts_table = pa.Table.from_pandas(df.sort_values(by=['date', 'event_ts']))
    pq.write_to_dataset(sorted_ts_table, root_path=out_sorted_ts, partition_cols=['date'], existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    
    # Induced failure: partitioned_by_user_id
    out_part_user = os.path.join(output_dir, 'partitioned_by_user_id')
    try:
        # this will create many files and may fail
        pq.write_to_dataset(enriched_table, root_path=out_part_user, partition_cols=['user_id'], existing_data_behavior='overwrite_or_ignore', max_partitions=200000)
    except Exception as e:
        print(f"Expected failure while partitioning by high cardinality user_id: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--out_dir', type=str, required=True)
    args = parser.parse_args()
    
    build_all_layouts(args.input, args.out_dir)

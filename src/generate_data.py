import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import os

def generate_events(num_rows: int, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    
    # 90 days span
    end_date = pd.Timestamp.now().floor('s')
    start_date = end_date - pd.Timedelta(days=90)
    
    countries = [
        'US', 'GB', 'CA', 'AU', 'IN', 'DE', 'FR', 'JP', 'BR', 'IT', 'ES', 'MX', 'NL', 'KR', 'CN',
        'SE', 'CH', 'BE', 'TR', 'TW', 'PL', 'AR', 'NO', 'AT', 'ZA', 'DK', 'FI', 'SG', 'IE', 'NZ',
        'PT', 'HK', 'MY', 'CL', 'CZ', 'GR', 'RO', 'HU', 'IL', 'CO', 'TH', 'UA', 'AE', 'VN', 'PE',
        'PH', 'EG', 'SA', 'MA', 'ID'
    ]
    event_types = ['click', 'view', 'purchase', 'login', 'logout', 'add_to_cart', 'search']
    devices = ['ios', 'android', 'web', 'windows', 'macos']
    
    batch_size = 1000000
    if num_rows < batch_size:
        batch_size = num_rows

    schema = pa.schema([
        ('event_ts', pa.timestamp('ms')),
        ('user_id', pa.int64()),
        ('country', pa.string()),
        ('event_type', pa.string()),
        ('device', pa.string()),
        ('value', pa.float64())
    ])

    writer = pq.ParquetWriter(out_path, schema, compression='snappy')
    
    rows_generated = 0
    np.random.seed(42)

    while rows_generated < num_rows:
        current_batch_size = min(batch_size, num_rows - rows_generated)
        
        # generate random data
        timestamps = start_date.value // 10**6 + np.random.randint(0, int((end_date - start_date).total_seconds() * 1000), size=current_batch_size, dtype=np.int64)
        
        users = np.random.randint(1, 1000000, current_batch_size)
        country_arr = np.random.choice(countries, current_batch_size)
        events_arr = np.random.choice(event_types, current_batch_size)
        device_arr = np.random.choice(devices, current_batch_size)
        values_arr = np.random.uniform(0.0, 1000.0, current_batch_size)

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(timestamps, type=pa.timestamp('ms')),
                pa.array(users, type=pa.int64()),
                pa.array(country_arr, type=pa.string()),
                pa.array(events_arr, type=pa.string()),
                pa.array(device_arr, type=pa.string()),
                pa.array(values_arr, type=pa.float64())
            ],
            schema=schema
        )
        
        writer.write_batch(batch)
        rows_generated += current_batch_size

    writer.close()
    print(f"Generated {num_rows} rows at {out_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows', type=int, default=1000000)
    parser.add_argument('--out_dir', type=str, required=True)
    args = parser.parse_args()
    
    generate_events(args.rows, os.path.join(args.out_dir, 'events.parquet'))

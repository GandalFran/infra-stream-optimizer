import os
import argparse
import hashlib
import json
import logging
import pandas as pd
import requests
import numpy as np
from pathlib import Path

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PREPARED_DIR = DATA_DIR / "prepared"

def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def download_file(url, dest_path):
    logger.info(f"Downloading {url} to {dest_path}")
    if dest_path.exists():
        logger.info("File already exists, skipping download.")
        return
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info("Download complete.")

def generate_event_id(row):
    # event_id = sha1(VendorID | pickup_ms | dropoff_ms | PULocationID | DOLocationID | total_amount | row_index)
    raw_str = f"{row.VendorID}|{row.pickup_ms}|{row.dropoff_ms}|{row.PULocationID}|{row.DOLocationID}|{row.total_amount}|{row.name}"
    return hashlib.sha1(raw_str.encode('utf-8')).hexdigest()

def prepare_dataset(month="2024-01", shards=4):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PREPARED_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"yellow_tripdata_{month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    file_path = RAW_DIR / filename

    try:
        download_file(url, file_path)
    except Exception as e:
        logger.error(f"Failed to download dataset: {e}")
        return

    checksum = calculate_checksum(file_path)
    logger.info(f"Checksum for {filename}: {checksum}")

    logger.info("Loading parquet file...")
    try:
        df = pd.read_parquet(file_path)
        original_rows = len(df)
        logger.info(f"Loaded {original_rows} rows.")

        # 1. Normalization
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        
        # Convert to epoch ms
        df['pickup_ms'] = df['tpep_pickup_datetime'].astype(np.int64) // 10**6
        df['dropoff_ms'] = df['tpep_dropoff_datetime'].astype(np.int64) // 10**6
        
        # Derived fields
        df['trip_duration_s'] = (df['dropoff_ms'] - df['pickup_ms']) / 1000.0

        # 2. Cleaning
        df = df[
            (df['trip_duration_s'] > 0) & 
            (df['trip_duration_s'] < 7200) & # 2 hours max
            (df['trip_distance'] > 0) & 
            (df['trip_distance'] < 200) &
            (df['passenger_count'] >= 0)
        ].head(50000) # Limit for verification speed
        filtered_rows = len(df)
        logger.info(f"Rows after filtering: {filtered_rows} (Dropped {original_rows - filtered_rows})")

        # 3. Add IDs and Key
        logger.info("Generating event IDs and Keys...")
        # Reset index to get unique row ID for hash
        df = df.reset_index(drop=True)
        df['event_id'] = df.apply(generate_event_id, axis=1)
        # Key: PULocationID (Hot partitions)
        df['key'] = df['PULocationID'].astype(str)
        df['event_time_ms'] = df['pickup_ms']

        # Select relevant columns for JSON
        cols = ['event_id', 'key', 'event_time_ms', 'pickup_ms', 'dropoff_ms', 
                'trip_distance', 'passenger_count', 'total_amount', 'PULocationID', 'DOLocationID']
        
        final_df = df[cols]

        # 4. Sharding
        logger.info(f"Sharding into {shards} files...")
        shards_list = []
        
        # Simple round-robin sharding using index
        final_df['shard_idx'] = final_df.index % shards
        
        for i in range(shards):
            shard_file = PREPARED_DIR / f"prepared_shard_{i:02d}.jsonl"
            shard_data = final_df[final_df['shard_idx'] == i].drop(columns=['shard_idx'])
            shard_data.to_json(shard_file, orient='records', lines=True)
            shards_list.append(str(shard_file))
            logger.info(f"Written {len(shard_data)} rows to {shard_file}")

        # 5. Manifest
        manifest = {
            "dataset_id": f"tlc_yellow_{month}",
            "original_file": filename,
            "checksum": checksum,
            "original_rows": original_rows,
            "filtered_rows": filtered_rows,
            "shards": shards_list,
            "generated_at": str(pd.Timestamp.now())
        }
        with open(PREPARED_DIR / "dataset_manifest.json", "w") as f:
            json.dump(manifest, f, indent=2)
            
        logger.info("Dataset preparation complete.")

    except Exception as e:
        logger.error(f"Error processing dataset: {e}")
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default="2024-01", help="YYYY-MM")
    parser.add_argument("--shards", type=int, default=4, help="Number of shards")
    args = parser.parse_args()
    
    prepare_dataset(month=args.month, shards=args.shards)

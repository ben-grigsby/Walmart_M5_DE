import os
import time
import random
import boto3
import pandas as pd

# ---------- CONFIGURATION ----------
source_file = "C:\\Users\\22beng\\Desktop\\Walmart_M5\\data\\sales_train_evaluation.csv"  
chunk_size = 10000
local_chunk_folder = "chunks"
bucket_name = "m5-walmart-project"
s3_prefix = "raw/"  # Folder path in S3
sleep_range = (5, 15)  # Seconds
# -----------------------------------

run_forever = False

# Ensure local chunk folder exists
os.makedirs(local_chunk_folder, exist_ok=True)

# Split original CSV into smaller chunks
def chunk_csv(source_file, chunk_size, output_folder):
    df = pd.read_csv(source_file)
    for idx, start in enumerate(range(0, len(df), chunk_size)):
        chunk = df.iloc[start:start+chunk_size]
        chunk_path = os.path.join(output_folder, f"chunk_{idx}.csv")
        chunk.to_csv(chunk_path, index=False)
    print(f"✅ Finished chunking {len(df)} rows into folder: {output_folder}")

# Upload a single file to S3
def upload_to_s3(local_path, bucket, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, s3_key)
    print(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")

# Delete all files under a prefix in S3
def clear_s3_prefix(bucket, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=bucket, Key=obj['Key'])
        print(f"🗑️ Deleted all files from s3://{bucket}/{prefix}")
    else:
        print(f"🟢 No files to delete in s3://{bucket}/{prefix}")

# ----------- MAIN LOOP ------------
chunk_csv(source_file, chunk_size, local_chunk_folder)

while True:
    # Step 1: Upload each chunk
    chunk_files = sorted(os.listdir(local_chunk_folder))
    i = 0
    for file in chunk_files:
        local_path = os.path.join(local_chunk_folder, file)
        s3_key = f"{s3_prefix}{file}"
        upload_to_s3(local_path, bucket_name, s3_key)
        time.sleep(random.randint(*sleep_range))
        i += 1
        print(i)

    print("🕒 All chunks uploaded. Waiting 5 seconds before restarting...")
    time.sleep(5)

    # Step 2: Delete chunks from S3
    # clear_s3_prefix(bucket_name, s3_prefix)

    if not run_forever:
        break

    print("🔁 Restarting upload cycle...\n")
    time.sleep(1)

    


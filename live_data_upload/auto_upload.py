import os
import time
import random
import boto3
import pandas as pd
from logger import get_logger

# ---------- CONFIGURATION ----------
source_file = "C:\\Users\\22beng\\Desktop\\Walmart_M5\\data\\sales_train_evaluation.csv"  
CHUNK_SIZE = 10000
LOCAL_CHUNK_FOLDER = "chunks"
bucket_name = "m5-walmart-project"
s3_prefix = "raw/"  # Folder path in S3
sleep_range = (5, 15)  # Seconds
logger = get_logger(__name__)
# -----------------------------------

run_forever = False

# Ensure local chunk folder exists
os.makedirs(LOCAL_CHUNK_FOLDER, exist_ok=True)

# Split original CSV into smaller chunks
def chunk_csv(source_file, chunk_size, output_folder):
    df = pd.read_csv(source_file)
    for idx, start in enumerate(range(0, len(df), chunk_size)):
        chunk = df.iloc[start:start+chunk_size]
        chunk_path = os.path.join(output_folder, f"{os.path.basename(source_file)[:-4]}_chunk_{idx}.csv")
        chunk.to_csv(chunk_path, index=False)
    logger.info(f"Chunking {source_file[:-4]}...")


# Chunk all files in a folder 

def folder_chunker(folder_path):
    files_in_folder = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]
    for file in files_in_folder:
        chunk_csv(file, CHUNK_SIZE, LOCAL_CHUNK_FOLDER)
    


# Upload a single file to S3
def upload_to_s3(local_path, bucket, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, s3_key)
    logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")

# Delete all files under a prefix in S3
def clear_s3_prefix(bucket, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=bucket, Key=obj['Key'])
        logger.info(f"Deleted all files from s3://{bucket}/{prefix}")
    else:
        logger.info(f"🟢 No files to delete in s3://{bucket}/{prefix}")

# ----------- MAIN LOOP ------------

# folder_chunker("C:\\Users\\22beng\\Desktop\\Walmart_M5\\data")

chunk_folder_path = "C:\\Users\\22beng\\Desktop\\Walmart_M5\\chunks"

i = 0
for file in os.listdir(chunk_folder_path):
    local_path = os.path.join(chunk_folder_path, file)
    s3_key = f"{s3_prefix}{file}"
    upload_to_s3(local_path, bucket_name, s3_key)
    time.sleep(1)
    i += 1
    print(i)


# while True:
#     # Step 1: Break each file in the folder down into chuns
#     folder_chunker("C:\\Users\\22beng\\Desktop\\Walmart_M5\\data")

#     # Step 2: Upload each chunk 
#     i = 0
#     for file in chunk_files:
#         local_path = os.path.join(local_chunk_folder, file)
#         s3_key = f"{s3_prefix}{file}"
#         upload_to_s3(local_path, bucket_name, s3_key)
#         time.sleep(random.randint(*sleep_range))
#         i += 1
#         print(i)

#     print("🕒 All chunks uploaded. Waiting 5 seconds before restarting...")
#     time.sleep(5)

#     # Step 2: Delete chunks from S3
#     # clear_s3_prefix(bucket_name, s3_prefix)

#     if not run_forever:
#         break

#     print("🔁 Restarting upload cycle...\n")
#     time.sleep(1)

    


import os 
import boto3

from collections import deque

# ===================================================================================
# Constants
# ===================================================================================

s3 = boto3.client("s3")
BUCKET_NAME = "m5-walmart-project"
PREFIX = 'raw/'
FOLDER_PATH = 'scripts/bronze/downloaded_data'
BATCH_SIZE = 5 

# ===================================================================================
# Functions
# ===================================================================================

def viewer(bucket_name=BUCKET_NAME):
    test =  s3.list_objects_v2(Bucket=bucket_name, Prefix=PREFIX)
    print(test['Contents'][0]['Key'])

def batch_download(bucket_name=BUCKET_NAME, prefix=PREFIX, folder_path=FOLDER_PATH, batch_size=BATCH_SIZE):
    contents = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']
    q = deque(contents)

    batched = 0

    while batched < batch_size:
        curr_key = q.popleft()['Key']
        basename = os.path.basename(curr_key)
        local_path = os.path.join(folder_path, basename)
        s3.download_file(bucket_name, curr_key, local_path)
        batched += 1
    
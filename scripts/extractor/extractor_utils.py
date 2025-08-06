import os 
import boto3
import sys

from concurrent.futures import ThreadPoolExecutor, as_completed

logs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
if logs_path not in sys.path:
    sys.path.append(logs_path)

from collections import deque

s3 = boto3.client('s3')

# ===================================================================================
# Functions
# ===================================================================================

def viewer(bucket_name, prefix):
    test =  s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    print(test['Contents'][0]['Key'])



def get_files_from_s3(bucket_name, prefix):
    contents = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']
    return contents



# def threaded_download(file_keys, max_threads=10):
#     with ThreadPoolExecutor(max_threads) as executor:
#         futures = [executor.submit(batch_download, key) for key in file_keys]
#         for future in as_completed(futures):
#             future.result()



def is_valid_key(bucket_name, key):
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise



def download_single_file(key, folder_path, bucket_name, logger, verbose=True):
    try:
        if not is_valid_key(key):
            logger.warning(f"Key not found in bucket: {key}")
            return 

        basename = os.path.basename(key)
        local_path = os.path.join(folder_path, basename)
        s3.download_file(bucket_name, key, local_path)

        if verbose:
            logger.info(f"Downloaded {basename} to {folder_path}")
    except Exception as e:
        logger.error(f"Failed to download {basename} to {folder_path}: {e}")



def batch_loop(batch_size, q, folder_path, bucket_name, logger):
    file_dict = {}
    for _ in range(min(batch_size, len(q))):
        key = q.popleft()['Key']
        basename = os.path.basename(key)
        local_path = os.path.join(folder_path, basename)
        s3.download_file(bucket_name, key, local_path)
        logger.info(f"Downloaded {basename} to {folder_path}")
        file_dict[basename] = local_path
    
    # print(file_dict)
    return file_dict, q



def create_file_queue(bucket_name, raw_prefix):
    contents = get_files_from_s3(bucket_name, raw_prefix)
    return deque(contents)


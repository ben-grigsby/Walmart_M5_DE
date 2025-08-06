import pandas as pd 
import os
import sys
import time

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(scripts_path)

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(base_path)

logs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
if logs_path not in sys.path:
    sys.path.append(logs_path)

from extractor.extractor_utils import (
    get_files_from_s3,
    batch_loop,
    create_file_queue,
)

from s3_utils import (
    upload_to_s3, 
    clear_s3_prefix,
    layer_data_upload,
)

from local_utils import (
    is_dir_empty,
    clean_folder,
    delete_file,
    access_all_files,
    convert_to_pd_df,
    convert_to_csv,
    convert_dict_dfs
)

from logs.logger import (
    batch_logger
)

from collections import deque

# ===================================================================================
# Constants
# ===================================================================================


# logger = get_logger()
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
DOWNLOAD_FOLDER_PATH = os.path.join(BASE_DIR, "downloaded_data")
UPLOAD_DATA_FOLDER = os.path.join(BASE_DIR, 'data_to_upload')
BRONZE_PREFIX = 'bronze_layer'

# ===================================================================================
# Functions
# ===================================================================================



def process_bronze(bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, logger, bucket_name, layer="bronze_"):
    for file in os.listdir(bronze_upload_data_folder):
        layer_data_upload(file, bronze_upload_data_folder, bronze_download_folder_path, bucket_name, logger, s3_bronze_prefix, layer)
  




def bronze_batch_process(logger, bucket_name, bronze_q, batch_size, bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, batch_num):
    if is_dir_empty(bronze_download_folder_path):
        logger.info(f"Starting batch {batch_num} - Remaining files: {len(bronze_q)}")
        file_dict, q = batch_loop(batch_size, bronze_q, bronze_download_folder_path, bucket_name, logger)

        dfs = convert_dict_dfs(file_dict)

        for filename, df in dfs.items():
            full_path = convert_to_csv(df, bronze_upload_data_folder, filename, logger, "bronze")

        process_bronze(bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, logger, bucket_name)
        clean_folder(bronze_download_folder_path, logger)
        clean_folder(bronze_upload_data_folder, logger)
    else:
        raise Exception(f"Directory {os.path.basename(bronze_download_folder_path)} is not empty. Clean it before starting the batch.")

    return q



def bronze_layer(bucket_name, raw_prefix, bronze_logger, batch_size, bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, layer_name):
    bronze_q = create_file_queue(bucket_name, raw_prefix)
    batch_num = 1

    while bronze_q:
        bronze_q = bronze_batch_process(bronze_logger, bucket_name, bronze_q, batch_size, bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, batch_num)
        batch_logger(bronze_logger, layer_name, batch_num)

        batch_num += 1
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
)

from s3_utils import (
    upload_to_s3, 
    clear_s3_prefix
)

from logs.logger import get_logger

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

def access_all_files(folder_path):
    """
    Accesses all files within a given folder 

    Input: 
        - folder_path: Directory to access the CSV
    Output: 
        - Dictionary with the key as the file name and the value as the filepath
    """
    files = {
        f"{os.path.splitext(f)[0]}": os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    }

    return files



def convert_to_pd_df(filepath):
    """
    Converts a file to a pandas df

    Input: 
        - filepath: Directory to access the CSV
    Output: 
        - Converted dataframe
    """
    
    return pd.read_csv(filepath)



def convert_to_csv(df, filepath, name, logger, prefix):
    """
    Converts a dataframe to a CSV file

    Input: 
        - df: Dataframe to be converted
        - filepath: Directory where the CSV should be saved
        - name: Name (without extensions) to use for the file
        - logger: Logger object for recording events
    Output:
        - The full path of the saved CSV file
    """    
    os.makedirs(filepath, exist_ok=True)
    full_path = os.path.join(filepath, f"{prefix}_{name}")
    if not os.path.exists(full_path):
        df.to_csv(full_path, index=False)
    else:
        logger.warning(f"{os.path.basename(full_path)} already in {os.path.basename(filepath)}")
    
    return full_path

def convert_dict_dfs(file_dict):
    dfs = {
        key: convert_to_pd_df(value)
        for key, value in file_dict.items()
    }

    return dfs



def delete_file(filepath, logger):
    """
    Deletes a file in the operating system
    Input:
        - filepath: Full path to the file to be deleted
    Output:
        - None
    """
    if os.path.isfile(filepath):
        os.remove(filepath)
        logger.info(f"Deleted: {filepath}")
    else:
        logger.warning(f"Unable to delete {os.path.basename(filepath)}, file not found: {filepath}")



def bronze_data_upload(file, bronze_upload_data_folder, bronze_download_folder_path, bucket_name, logger, s3_bronze_prefix):
    full_path_upload = os.path.join(bronze_upload_data_folder, file)
    if file.startswith("bronze_"):
        original_file = file.replace("bronze_", "", 1)
    full_path_download = os.path.join(bronze_download_folder_path, original_file)

    s3_key = f"{s3_bronze_prefix}/{file}"
    print(s3_key)
    upload_to_s3(full_path_upload, bucket_name, s3_key, logger)
    
    return full_path_upload, full_path_download


def process_bronze(bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, logger, bucket_name):
    for file in os.listdir(bronze_upload_data_folder):
        full_path_upload, full_path_download = bronze_data_upload(file, bronze_upload_data_folder, bronze_download_folder_path, bucket_name, logger, s3_bronze_prefix)
        delete_file(full_path_upload, logger)
        delete_file(full_path_download, logger)



def is_dir_empty(path):
    return len(os.listdir(path)) == 0



def bronze_batch_process(logger, bucket_name, bronze_q, batch_size, bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, batch_num):
    if is_dir_empty(bronze_download_folder_path):
        logger.info(f"Starting batch {batch_num} - Remaining files: {len(bronze_q)}")
        file_dict = batch_loop(batch_size, bronze_q, bronze_download_folder_path, bucket_name, logger)

        dfs = convert_dict_dfs(file_dict)

        for filename, df in dfs.items():
            full_path = convert_to_csv(df, bronze_upload_data_folder, filename, logger, "bronze")

        process_bronze(bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, logger, bucket_name)

    else:
        raise Exception(f"Directory {os.path.basename(bronze_download_folder_path)} is not empty. Clean it before starting the batch.")



def batch_logger(logger, batch_num):
    logger.info(f"Completed Bronze layer for {batch_num} - Moving onto Silver Layer")
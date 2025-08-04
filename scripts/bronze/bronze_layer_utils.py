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

    BUCKET_NAME,
    PREFIX,
    BATCH_SIZE,
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



def convert_to_csv(df, filepath, name, logger):
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
    full_path = os.path.join(filepath, f"bronze_{name}")
    if not os.path.exists(full_path):
        df.to_csv(full_path, index=False)
    else:
        logger.warning(f"{os.path.basename(full_path)} already in {os.path.basename(filepath)}")
    
    return full_path



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



def batch_download(logger, bucket_name=BUCKET_NAME, prefix=PREFIX, batch_size=BATCH_SIZE, upload_data_folder=UPLOAD_DATA_FOLDER, download_folder_path=DOWNLOAD_FOLDER_PATH, s3_bronze_prefix=BRONZE_PREFIX):
    contents = get_files_from_s3(bucket_name, prefix)
    q = deque(contents)
    total_file_count = len(contents)
    batch_num = 1

    while q:
        logger.info(f"Starting batch {batch_num} - Remaining files: {len(q)}")
        file_dict = batch_loop(batch_size, q)
        downloaded = total_file_count - len(q)

        dfs = {
            key: convert_to_pd_df(value)
            for key, value in file_dict.items()
        }

        for filename, df in dfs.items():
            full_path = convert_to_csv(df, upload_data_folder, filename, logger)

        for file in os.listdir(UPLOAD_DATA_FOLDER):
            full_path_upload = os.path.join(upload_data_folder, file)
            if file.startswith("bronze_"):
                original_file = file.replace("bronze_", "", 1)
            full_path_download = os.path.join(download_folder_path, original_file)
            s3_key = f"{s3_bronze_prefix}/{file}"
            upload_to_s3(full_path_upload, bucket_name, s3_key, logger)
            delete_file(full_path_upload, logger)
            delete_file(full_path_download, logger)
            
        


        logger.info(f"Batched {downloaded}/{total_file_count} files")
        batch_num += 1

        time.sleep(20)



#### Two options, though one seems much better as of right now. I convert the file to pd and then back to .csv and save
#### under a different file name that helps us determine that the file has been processed. I should probably delete the
#### file after doing that as we no longer need that raw data since we have converted it to a 'cleaned' csv. The other 
#### option is to just delete it at the end. Not exactly sure which one would be better speed wise but I am fairly certain
#### I know which one would be better memory wise. That is what I am going to do. Also, I will create a new folder with 
#### dedicated s3 functions so I don't need to have numerous repeats and can just pull the upload and download stuff from 
#### one central folder.

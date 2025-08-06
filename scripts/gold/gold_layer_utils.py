import os
import sys

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(base_path)

from extractor.extractor_utils import (
    create_file_queue,
    batch_loop,
)

from scripts.s3_utils import (
    layer_data_upload,
)

from local_utils import (
    delete_file,
    clean_folder,
    is_dir_empty,
    convert_dict_dfs,
    convert_to_csv,
)

from logs.logger import (
    batch_logger
)

# ===================================================================================
# Functions
# ===================================================================================

def gold_batch_process(gold_download_folder_path, logger, batch_num, batch_size, gold_q, bucket_name):
    if is_dir_empty(gold_download_folder_path):
        logger.info(f"Starting batch {batch_num}")
        file_dict, q = batch_loop(batch_size, gold_q, gold_download_folder_path, bucket_name, logger)
    
        dfs = convert_dict_dfs(file_dict)

        for filename, df in dfs.items():
            
import os
import sys

from collections import deque

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(base_path)

from logs.logger import (
    get_logger,
)

from scripts.extractor.extractor_utils import (
    batch_loop,
    create_file_queue,
)

from scripts.bronze.bronze_layer_utils import (
    convert_to_pd_df,
    convert_to_csv,
    process_bronze, 
    convert_dict_dfs,
    is_dir_empty,
    bronze_batch_process,
    batch_logger,
)

from scripts.silver.silver_layer_utils import (
    silver_batch_process,
)

# from scripts.s3_utils import (
    
# )

# ===================================================================================
# Functions
# ===================================================================================


# =================== Overall Pipeline ===================

def overall_pipeline(
        bucket_name, 
        raw_prefix, 
        bronze_logger, 
        batch_size, 
        bronze_upload_data_folder, 
        bronze_download_folder_path, 
        s3_bronze_prefix, 
        bronze_prefix,
        silver_download_folder_path, 
        silver_upload_data_folder,
        silver_logger, 
        s3_silver_prefix
):
    bronze_q = create_file_queue(bucket_name, raw_prefix)
    batch_num = 1

    while bronze_q:
        bronze_batch_process(bronze_logger, bucket_name, bronze_q, batch_size, bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, batch_num)
        batch_logger(bronze_logger, batch_num)

        # ============== Silver Layer ==============
        silver_logger.info(f"Beginning Silver Layer for {batch_num}")
        silver_q = create_file_queue(bucket_name, bronze_prefix)
        silver_batch_process(silver_download_folder_path, silver_logger, batch_num, batch_size, silver_q, bucket_name, silver_upload_data_folder, s3_silver_prefix)


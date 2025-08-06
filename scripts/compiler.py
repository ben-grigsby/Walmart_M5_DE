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
    bronze_layer,
)

from scripts.silver.silver_layer_utils import (
    silver_layer,
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
        bronze_layer_name,

        bronze_prefix,
        silver_download_folder_path, 
        silver_upload_data_folder,
        silver_logger, 
        s3_silver_prefix
):
    # print("\n")
    # print("=====================================================")
    # print("=================== Bronze Layer ====================")
    # print("=====================================================")
    # print("\n")

    # bronze_layer(bucket_name, raw_prefix, bronze_logger, batch_size, bronze_upload_data_folder, bronze_download_folder_path, s3_bronze_prefix, bronze_layer_name)
    
    print("\n")
    print("=====================================================")
    print("=================== Silver Layer ====================")
    print("=====================================================")
    print("\n")
    
    silver_layer(bucket_name, bronze_prefix, silver_logger, batch_size, silver_upload_data_folder, silver_download_folder_path, s3_silver_prefix)
    # gold_layer()

        # # ============== Silver Layer ==============
        # silver_logger.info(f"Beginning Silver Layer for batch {batch_num}")
        # silver_q = create_file_queue(bucket_name, bronze_prefix)
        # silver_batch_process(silver_download_folder_path, silver_logger, batch_num, batch_size, silver_q, bucket_name, silver_upload_data_folder, s3_silver_prefix)

        # batch_num += 1
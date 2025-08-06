from scripts.compiler import (
    overall_pipeline
)

from configs import (
    bronze_logger,
    BUCKET_NAME,
    RAW_PREFIX,
    BATCH_SIZE,
    BRONZE_UPLOAD_DATA_FOLDER,
    BRONZE_DOWNLOAD_FOLDER_PATH,
    BRONZE_LAYER_NAME,
    
    S3_BRONZE_PREFIX,
    bronze_logger,
    BRONZE_PREFIX,
    SILVER_UPLOAD_DATA_FOLDER,
    SILVER_DOWNLOAD_FOLDER_PATH,
    silver_logger,
    S3_SILVER_PREFIX
)

# ===================================================================================
# Main
# ===================================================================================

overall_pipeline(
    BUCKET_NAME, 
    RAW_PREFIX, 
    bronze_logger, 
    BATCH_SIZE, 
    BRONZE_UPLOAD_DATA_FOLDER, 
    BRONZE_DOWNLOAD_FOLDER_PATH,
    S3_BRONZE_PREFIX,
    BRONZE_LAYER_NAME,

    BRONZE_PREFIX,
    SILVER_DOWNLOAD_FOLDER_PATH,
    SILVER_UPLOAD_DATA_FOLDER,
    silver_logger,
    S3_SILVER_PREFIX,
)

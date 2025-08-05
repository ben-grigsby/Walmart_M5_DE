from logs.logger import (
    get_logger
)

# ===================================================================================
# Constants
# ===================================================================================

BUCKET_NAME = "m5-walmart-project"
RAW_PREFIX = 'raw/'
BATCH_SIZE = 5
BRONZE_UPLOAD_DATA_FOLDER = 'scripts/bronze/bronze_data_ready_for_upload'
BRONZE_DOWNLOAD_FOLDER_PATH = 'scripts/bronze/bronze_downloaded_data'
S3_BRONZE_PREFIX = 'bronze'
bronze_logger = get_logger("bronze_logger")
BRONZE_PREFIX = 'bronze/'
SILVER_UPLOAD_DATA_FOLDER = 'scripts/silver/silver_data_ready_for_upload'
SILVER_DOWNLOAD_FOLDER_PATH = 'scripts/silver/silver_downloaded_data'
silver_logger = get_logger("silver_logger")
S3_SILVER_PREFIX = 'silver'





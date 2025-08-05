import os
import sys

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(scripts_path)

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(base_path)

from logs.logger import (
    get_logger,
)


from s3_utils import (
    upload_to_s3, 
    clear_s3_prefix
)

from bronze_layer_utils import (
    batch_download
)

# ===================================================================================
# Constants
# ===================================================================================

# BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # where bronze_main.py is
# DOWNLOADED_DATA_FOLDER = os.path.join(BASE_DIR, 'downloaded_data')
# UPLOAD_DATA_FOLDER = os.path.join(BASE_DIR, 'data_to_upload')
# BUCKET = 'm5-walmart-project'
# s3_bronze_prefix = 'bronze_layer'

logger = get_logger("bronze_main")


# ===================================================================================
# __main__
# ===================================================================================


batch_download(logger)

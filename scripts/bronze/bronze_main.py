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
    convert_to_csv,
    convert_to_pd_df,
    delete_file,
    access_all_files
)

# ===================================================================================
# Constants
# ===================================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # where bronze_main.py is
DOWNLOADED_DATA_FOLDER = os.path.join(BASE_DIR, 'downloaded_data')
UPLOAD_DATA_FOLDER = os.path.join(BASE_DIR, 'data_to_upload')
BUCKET = 'm5-walmart-project'
s3_bronze_prefix = 'bronze_layer'

logger = get_logger("bronze_main")


# ===================================================================================
# __main__
# ===================================================================================

file_dict = access_all_files(DOWNLOADED_DATA_FOLDER)



dfs = {
    key: convert_to_pd_df(value)
    for key, value in file_dict.items()
}

for filename, df in dfs.items():
    convert_to_csv(df, UPLOAD_DATA_FOLDER, filename, logger)
    print(filename)

for file in os.listdir(UPLOAD_DATA_FOLDER):
    full_path = os.path.join(UPLOAD_DATA_FOLDER, file)
    s3_key = f"{s3_bronze_prefix}/{file}"
    upload_to_s3(full_path, BUCKET, s3_key, logger)


#### Figure out how to use parallelism so I don't need to go through the 
#### files one step at a time and can instead parallelism them.

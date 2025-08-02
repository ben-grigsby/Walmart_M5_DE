import os
import sys

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append(scripts_path)

# from s3_utils import upload_to_s3, clear_s3_prefi
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
DATA_FOLDER = os.path.join(BASE_DIR, 'downloaded_data')


# ===================================================================================
# __main__
# ===================================================================================

file_dict = access_all_files(DATA_FOLDER)

dfs = {
    key: convert_to_pd_df(value)
    for key, value in file_dict.items()
}

for filename, df in dfs.items():
    convert_to_csv(df, DATA_FOLDER, filename)

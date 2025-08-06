import os 
import sys
import pandas as pd

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


def melt_file(df, id_vars, value_vars, var_name, value_name):
    """
    Melts a dataframe from wide to long format.

    Parameters:
        df (pd.DataFrame): The dataframe to melt.
        id_vars (list): Columns to keep fixed (not unpivoted).
        value_vars (list): Columns to melt.
        var_name (str): Name for the 'variable' column (default: "variable").
        value_name (str): Name for the 'value' column (default: "value").

    Returns:
        pd.DataFrame: Melted dataframe.
    """
    melted_df = pd.melt(
        df,
        id_vars=id_vars, 
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name
    )

    return melted_df 



def df_melt_vars(df):
    id_vars = [col for col in df.columns if not col.startswith("d_")]
    value_vars = [col for col in df.columns if col.startswith("d_")]
    var_name = "day"
    value_name = "sales"

    return id_vars, value_vars, var_name, value_name



### Extremely simple logic for determining which files need to be melted as I am still currently working on level 1 code
def identify_df_to_melt(df):
    for col in df.columns:
        if col.startswith("d_"):
            return True
    return False




def process_silver(silver_upload_data_folder, silver_download_folder_path, bucket_name, logger, s3_silver_prefix, layer="silver_"):
    # print(silver_download_folder_path)
    for file in os.listdir(silver_upload_data_folder):
        full_path_upload = layer_data_upload(file, silver_upload_data_folder, silver_download_folder_path, bucket_name, logger, s3_silver_prefix, layer)
        # delete_file(full_path_upload, logger)

        # download_dir = os.path.dirname(full_path_download)
        # # print(download_dir, full_path_download)
        # download_basename = os.path.basename(full_path_download)
        # replaced_basename = download_basename.replace("melted", "bronze")

        # full_path_download = os.path.join(download_dir, replaced_basename)
        # delete_file(full_path_download, logger)



def silver_batch_process(silver_download_folder_path, logger, batch_num, batch_size, silver_q, bucket_name, silver_upload_data_folder, s3_silver_prefix):
    if is_dir_empty(silver_download_folder_path):
        logger.info(f"Starting batch {batch_num}")
        file_dict, q = batch_loop(batch_size, silver_q, silver_download_folder_path, bucket_name, logger)
        
        dfs = convert_dict_dfs(file_dict)
        updated_dict = {}

        for filename, df in dfs.items():
            base = os.path.basename(filename)
            trim_name = base.replace("bronze_", "")
            if identify_df_to_melt(df):
                id_vars, value_vars, var_name, value_name = df_melt_vars(df)
                logger.info(f"Melting {os.path.basename(filename)}")
                df_to_save = melt_file(df, id_vars, value_vars, var_name, value_name)

                silver_name = trim_name

                ac_name = os.path.splitext(silver_name)[0]
                output_name = f"melted_{ac_name}.csv"

            else:
                df_to_save = df
                output_name = os.path.basename(trim_name)
            
            updated_dict[output_name] = df_to_save
            full_path = convert_to_csv(df_to_save, silver_upload_data_folder, output_name, logger, "silver")


        process_silver(silver_upload_data_folder, silver_download_folder_path, bucket_name, logger, s3_silver_prefix)
        clean_folder(silver_download_folder_path, logger)
        clean_folder(silver_upload_data_folder, logger)

        return q

    else:
        raise Exception (f"Directory {os.path.basename(silver_download_folder_path)} is not empty. Clean it before pushing through silver layer.")
        


def silver_layer(bucket_name, bronze_prefix, silver_logger, batch_size, silver_upload_data_folder, silver_download_folder_path, s3_silver_prefix):
    silver_q = create_file_queue(bucket_name, bronze_prefix)
    batch_num = 1

    while silver_q:
        silver_q = silver_batch_process(silver_download_folder_path, silver_logger, batch_num, batch_size, silver_q, bucket_name, silver_upload_data_folder, s3_silver_prefix)
        batch_logger(silver_logger, batch_num)

        batch_num += 1
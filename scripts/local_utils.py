import os
import pandas as pd

# ===================================================================================
# Functions
# ===================================================================================


def delete_file(filepath, logger):
    """
    Deletes a file in the operating system
    Input:
        - filepath: Full path to the file to be deleted
    Output:
        - None
    """
    print(filepath)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logger.info(f"Deleted: {filepath}")
    else:
        logger.warning(f"Unable to delete {os.path.basename(filepath)}, file not found: {filepath}")



def clean_folder(folder, logger):

    for file in os.listdir(folder):
        full_path = os.path.join(folder, file)
        delete_file(full_path, logger)
    


def is_dir_empty(path):
    return len(os.listdir(path)) == 0



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



def convert_to_csv(df, filepath, name, logger, prefix):
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
    full_path = os.path.join(filepath, f"{prefix}_{name}")
    if not os.path.exists(full_path):
        df.to_csv(full_path, index=False)
    else:
        logger.warning(f"{os.path.basename(full_path)} already in {os.path.basename(filepath)}")
    
    return full_path

def convert_dict_dfs(file_dict):
    dfs = {
        key: convert_to_pd_df(value)
        for key, value in file_dict.items()
    }

    return dfs
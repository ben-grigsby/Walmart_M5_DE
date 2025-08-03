import pandas as pd 
import os
import sys

# ===================================================================================
# Constants
# ===================================================================================

FOLDER_PATH = "downloaded_data"

# ===================================================================================
# Functions
# ===================================================================================

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


def convert_to_csv(df, filepath, name, logger):
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
    full_path = os.path.join(filepath, f"bronze_{name}.csv")
    if not os.path.exists(full_path):
        df.to_csv(full_path, index=False)
    else:
        logger.warning(f"{os.path.basename(full_path)} already in {os.path.basename(filepath)}")
    
    return full_path


def delete_file(filepath):
    """
    Deletes a file in the operating system
    Input:
        - filepath: Full path to the file to be deleted
    Output:
        - None
    """
    if os.path.isfile(filepath):
        os.remove(filepath)
        print(f"Deleted: {filepath}")
    else:
        print(f"File not found: {filepath}")




#### Two options, though one seems much better as of right now. I convert the file to pd and then back to .csv and save
#### under a different file name that helps us determine that the file has been processed. I should probably delete the
#### file after doing that as we no longer need that raw data since we have converted it to a 'cleaned' csv. The other 
#### option is to just delete it at the end. Not exactly sure which one would be better speed wise but I am fairly certain
#### I know which one would be better memory wise. That is what I am going to do. Also, I will create a new folder with 
#### dedicated s3 functions so I don't need to have numerous repeats and can just pull the upload and download stuff from 
#### one central folder.

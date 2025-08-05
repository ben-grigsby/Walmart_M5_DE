import boto3


# ===================================================================================
# Constants
# ===================================================================================



# ===================================================================================
# functions
# ===================================================================================

def upload_to_s3(local_path, bucket, s3_key, logger):
    """
    Uploads one file to the specificied bucket on s3

    Input:
        - local_path: Directory for which the file is stored
        - bucket: Storage name of the file on S3
        - s3_key: Key used to access S3
    """

    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, s3_key)
    logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")


def clear_s3_prefix(bucket, prefix, logger):
    """
    Deletes all files with a specific prefix from the bucket on s3
    """
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=bucket, Key=obj['Key'])
        logger.info(f"Deleted all files from s3://{bucket}/{prefix}")
    else:
        logger.info(f"🟢 No files to delete in s3://{bucket}/{prefix}")




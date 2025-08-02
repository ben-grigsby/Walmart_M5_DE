import boto3


# ===================================================================================
# Constants
# ===================================================================================



# ===================================================================================
# functions
# ===================================================================================

def upload_to_s3(local_path, bucket, s3_key):
    """
    Uploads one file to the specificied bucket on s3
    """

    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, s3_key)
    logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")


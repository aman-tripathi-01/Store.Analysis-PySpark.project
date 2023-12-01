import os
from resources.dev import config
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *

s3_client_provider = S3ClientProvider(
    decrypt(config.aws_access_key), decrypt(config.aws_secret_key)
)
s3_client = s3_client_provider.get_client()

local_file_path = (
    "D:\\Projects\\PycharmProjects\\Spark_project_1\\data\\sales_data_to_s3\\"
)


def upload_to_s3(s3_directory, s3_bucket, local_file_path):
    s3_prefix = f"{s3_directory}"
    try:
        logger.info(f"Uploading the file/s to S3 bucket: {s3_bucket}/{s3_directory}")
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                logger.info(f"Uploading file: {file}")
                local_file_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}{file}"
                s3_client.upload_file(local_file_path, s3_bucket, s3_key)
                logger.info("Above file was uploaded to S3 successfully.")
    except Exception as e:
        logger.error("Data upload failed with error: %s", e)
    logger.info("All files uploaded successfully.")


s3_directory = "sales_data/"
s3_bucket = config.bucket_name
upload_to_s3(s3_directory, s3_bucket, local_file_path)

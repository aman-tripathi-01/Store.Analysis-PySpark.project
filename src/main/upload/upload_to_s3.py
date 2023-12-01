from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import *
from resources.dev import config
import traceback
import datetime
import os

from src.main.utility.s3_client_object import S3ClientProvider


class UploadToS3:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def upload_to_s3(self, s3_directory, s3_bucket, local_file_path):
        current_epoch = int(datetime.datetime.now().timestamp()) * 100
        s3_prefix = f"{s3_directory}{current_epoch}/"
        try:
            for root, dirs, files in os.walk(local_file_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    s3_key = f"{s3_prefix}{file}"
                    self.s3_client.upload_file(local_file_path, s3_bucket, s3_key)
            return f"Data Successfully uploaded in {s3_directory} data mart "
        except Exception as e:
            logger.error(f"Error uploading file : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e

    def partitioned_data_upload_to_s3(self, s3_directory, s3_bucket, local_file_path):
        current_epoch = int(datetime.datetime.now().timestamp()) * 100
        s3_prefix = f"{s3_directory}{current_epoch}/"
        try:
            for root, dirs, files in os.walk(local_file_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_file_path = os.path.relpath(
                        local_file_path,
                        config.sales_team_data_mart_partitioned_local_file,
                    )
                    relative_file_path = relative_file_path.replace("\\","/")
                    # print("RFP:", relative_file_path)
                    s3_key = f"{s3_prefix}{relative_file_path}"
                    # print("S3 Key:", s3_key)
                    self.s3_client.upload_file(local_file_path, s3_bucket, s3_key)
                    # print("File uploaded")
            return f"Data Successfully uploaded in {s3_directory} data mart "
        except Exception as e:
            logger.error(f"Error uploading file : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e

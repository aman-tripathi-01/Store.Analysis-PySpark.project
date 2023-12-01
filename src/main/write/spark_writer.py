import traceback
from src.main.utility.logging_config import *


class SparkWriter:
    def __init__(self, mode, data_format):
        self.mode = mode
        self.data_format = data_format

    def spark_writer(self, df, file_path):
        try:
            df.write.format(self.data_format).option("header", "true").mode(
                self.mode
            ).option("path", file_path).save()
        except Exception as e:
            logger.error(f"Error writing the data : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e

    # Writing spark data if data is to be partitioned
    def spark_partition_writer(self, df, file_path, list_of_partitions):
        try:
            df.write.format(self.data_format).option("header", "true").mode(
                self.mode
            ).partitionBy(list_of_partitions).option("path", file_path).save()
        except Exception as e:
            logger.error(f"Error writing the data : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e

import os.path
import shutil
import sys
from datetime import datetime
from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import (
    customer_mart_calculation_table_write,
)
from src.main.transformations.jobs.dimenssion_tables_join import dimensions_table_join
from src.main.transformations.jobs.sales_team_mart_sql_transform_write import (
    sales_team_mart_calculation_table_write,
)
from src.main.utility.logging_config import *
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.download.aws_file_download import *
from src.main.utility.spark_session import *
from src.main.write.spark_writer import SparkWriter

# Fetching Keys
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

# List of Buckets on S3
s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
logger.info("List of Buckets: %s", response["Buckets"])

# Creating SQL connection
csv_files = [
    file for file in os.listdir(config.local_directory) if file.endswith(".csv")
]
connection = get_mysql_connection()
cursor = connection.cursor()

# Executing the SQL command to check inactive files.
total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    logger.info("Fetched list of files, starting sql execution")
    statement = f"""
    select distinct file_name 
    from {config.database_name}.{config.product_staging_table}
    where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A';
    """
    logger.info(f"Dynamically created statement: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.warning("Your last run failed, Please check!")
    else:
        logger.info("No record found in the file.")
else:
    logger.info("Last run was successful.")

# Getting the Absolute file path of the files on S3 and downloading them.
try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(
        s3_client, config.bucket_name, folder_path
    )
    logger.info(f"Absolute Path of S3 bucket for csv file: {s3_absolute_file_path}")
    if not s3_absolute_file_path:
        logger.info(f"No file is present at {folder_path}")
        raise Exception("No data available to process.")
except Exception as e:
    logger.error("Exited with error: %s", e)
    raise e

prefix = f"s3://{config.bucket_name}/"
file_paths = [url[len(prefix) :] for url in s3_absolute_file_path]
logger.info(
    f"File path available on S3 under '{config.bucket_name}' bucket and folder name is {file_paths}"
)
try:
    downloader = S3FileDownloader(s3_client, config.bucket_name, config.local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("Error occurred during downloading file/s from S3: %s", e)
    sys.exit()

all_files = os.listdir(config.local_directory)
logger.info(f"List of file present at my local directory are: {all_files}")

# Keeping only the '.csv' files
if all_files:
    csv_files = []
    error_files = []
    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(
                os.path.abspath(os.path.join(config.local_directory, file))
            )
        else:
            error_files.append(
                os.path.abspath(os.path.join(config.local_directory, file))
            )
    if len(csv_files) == 0:
        logger.error("No csv data available to process the request.")
        raise Exception("No csv data available to process the request.")
else:
    logger.error("No data available for processing.")
    raise Exception("No data available for processing.")

logger.info("************************ Listing the CSV files ************************")
logger.info(f"List of csv files to process: {csv_files}")

# Creating Spark Session
logger.info("************************ Creating Spark session ************************")
spark = spark_session()
logger.info(
    "************************ Spark session created successfully ************************"
)

# Checking the Schema of the data in the csv files
logger.info(
    "************************ checking Schema in files downloaded from S3 ************************"
)
correct_files = list()
for data in csv_files:
    base_filename = os.path.basename(data)
    data_schema = spark.read.format("csv").option("header", "true").load(data).columns
    logger.info(f"Schema for the {base_filename} is {data_schema}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)

    if missing_columns:
        error_files.append(data)
        logger.warning(f"Missing columns are {missing_columns} in file: {data}")
    else:
        logger.info(f"No missing column in {base_filename}")
        correct_files.append(data)
logger.info(f"*********** List of correct files: *********** {correct_files}")
logger.info(f"*********** List of error files: *********** {error_files}")

# Moving the error files to local directory.
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path, destination_path)
            logger.info(f"On Local: Moved error file {file_name} to {destination_path}")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(
                s3_client,
                config.bucket_name,
                source_prefix,
                destination_prefix,
                file_name,
            )
            logger.info(f"On S3: {message}")
        else:
            logger.error(f"{file_path} doesn't exist.")

else:
    logger.info("*********** No Error files available in our dataset. *********** ")

# Updating the staging table before processing the file for data
logger.info(
    "*********** Updating product_staging_table with status that the process is started  ***********"
)
insert_statements = list()
db_name = config.database_name
current_date = datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        statements = f"""\
        INSERT INTO {db_name}.{config.product_staging_table}\
        (file_name, file_location, created_date, status)\
        VALUES ('{file_name}', '{file}', '{formatted_date}', 'A')      
        """
        insert_statements.append(statements)
    logger.info(f"Insert statement created: {insert_statements}")
    logger.info("************* Connecting with MySQL server *************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("************* Connected to MySQL successfully. *************")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("************* No file to process *************")
    raise Exception(
        "************* No file present in correct_files list for processing *************"
    )
logger.info("************* Staging table updated successfully. *************")

# Fixing extra columns from the input csv files, if present
logger.info("************* Fixing extra columns coming from source *************")
my_schema = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("sales_date", DateType(), True),
        StructField("sales_person_id", IntegerType(), True),
        StructField("price", FloatType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_cost", FloatType(), True),
        StructField("additional_columns", StringType(), True),
    ]
)

final_df_to_process = spark.createDataFrame([], schema=my_schema)
# final_df_to_process.show()
for file in correct_files:
    data_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferschema", "true")
        .load(file)
    )
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present in {os.path.basename(file)}: {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn(
            "additional_column", concat_ws(", ", *extra_columns)
        ).select(
            "customer_id",
            "store_id",
            "product_name",
            "sales_date",
            "sales_person_id",
            "price",
            "quantity",
            "total_cost",
            "additional_column",
        )
        logger.info(f"Processed {file} and added additional column.")
    else:
        data_df = data_df.withColumn("additional_column", lit(None)).select(
            "customer_id",
            "store_id",
            "product_name",
            "sales_date",
            "sales_person_id",
            "price",
            "quantity",
            "total_cost",
            "additional_column",
        )
    final_df_to_process = final_df_to_process.union(data_df)
logger.info(
    "************* Final DataFrame that will be processed from source *************"
)
# print(final_df_to_process.count())
# final_df_to_process.show(100)

# Connecting with DatabaseReader and creating DataFrames for all tables
database_client = DatabaseReader(config.url, config.properties)

logger.info("Loading customer table into customer_table_df")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
logger.info("Loading product table into product_table_df")
product_table_df = database_client.create_dataframe(spark, config.product_table)
logger.info("Loading product staging table into product_staging_table_df")
product_staging_table_df = database_client.create_dataframe(
    spark, config.product_staging_table
)
logger.info("Loading sales_team table into sales_team_table_df")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)
logger.info("Loading store table into store_table_df")
store_table_df = database_client.create_dataframe(spark, config.store_table)
logger.info("******************* Joining tables *******************")
s3_customer_store_sales_df_join = dimensions_table_join(
    final_df_to_process, customer_table_df, store_table_df, sales_team_table_df
)
logger.info(
    f"*********** Final Enriched Data count: {s3_customer_store_sales_df_join.count()} ***********"
)

# Writing customer_data into customer data mart in local and then transferring it to S3 bucket.
logger.info("*********** Writing data to Customer Data Mart ***********")
final_customer_data_mart_df = s3_customer_store_sales_df_join.select(
    "customer_id",
    "customer_first_name",
    "customer_last_name",
    "customer_address",
    "customer_pincode",
    "phone_number",
    "sales_date",
    "total_cost",
)
logger.info("Final Data for Customer Data Mart")
print(final_customer_data_mart_df.show(5))

parquet_writer = SparkWriter("overwrite", "parquet")
parquet_writer.spark_writer(
    final_customer_data_mart_df, config.customer_data_mart_local_file
)
logger.info("Customer Data written to local disk.")
logger.info("Moving customer data to S3...")
s3_upload = UploadToS3(s3_client)
response = s3_upload.upload_to_s3(
    config.s3_customer_datamart_directory,
    config.bucket_name,
    config.customer_data_mart_local_file,
)
logger.info(f"{response}")

# Writing sales_team data into sales_team data mart in local and then transferring it to S3 bucket.
logger.info("*********** Writing data to Sales team Data Mart ***********")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join.select(
    "sales_person_id",
    "sales_person_first_name",
    "sales_person_last_name",
    "store_manager_name",
    "manager_id",
    "is_manager",
    "sales_person_address",
    "sales_person_pincode",
    "store_id",
    "total_cost",
    "sales_date",
    expr("SUBSTRING(sales_date,1,7) as sale_month"),
)
logger.info("Final Data for Sales team Data Mart")
print(final_sales_team_data_mart_df.show(5))

parquet_writer = SparkWriter("overwrite", "parquet")
parquet_writer.spark_writer(
    final_sales_team_data_mart_df, config.sales_team_data_mart_local_file
)
logger.info("Sales team Data written to local disk.")
logger.info("Moving Sales team data to S3...")
response = s3_upload.upload_to_s3(
    config.s3_sales_datamart_directory,
    config.bucket_name,
    config.sales_team_data_mart_local_file,
)
logger.info(f"{response}")

# Writing the data in partitions
partition_by_list = ["sale_month", "store_id"]
parquet_writer.spark_partition_writer(
    final_sales_team_data_mart_df,
    config.sales_team_data_mart_partitioned_local_file,
    partition_by_list
)
logger.info("Sales team Data written to local disk.")
logger.info("Moving Sales team data to S3...")
response = s3_upload.partitioned_data_upload_to_s3(
    config.s3_sales_partitioned_data_mart_directory,
    config.bucket_name,
    config.sales_team_data_mart_partitioned_local_file,
)
logger.info(f"{response}")

logging.info("Calculating total_purchased_amount for each customer per month")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info(
    "Calculation of customer's monthly expenditure done and data written to MySQL."
)

logging.info("Calculating total_sales for each member of sales_team per month")
sales_team_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info(
    "Calculation of customer's monthly expenditure done and data written to MySQL."
)


# Moving the processed files on S3 to processed directory
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
response = move_s3_to_s3(
    s3_client, config.bucket_name, source_prefix, destination_prefix
)
logger.info(f"{response}")

logger.info("****************** Deleting Local files ******************")
try:
    delete_local_file(config.local_directory)
    delete_local_file(config.customer_data_mart_local_file)
    delete_local_file(config.sales_team_data_mart_local_file)
    delete_local_file(config.sales_team_data_mart_partitioned_local_file)
    delete_local_file(config.local_directory)
except Exception as e:
    logger.error(f"Error occurred while deleting the file from local: {e}")

# Updating staging table
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f""" UPDATE {db_name}.{config.product_staging_table} SET status = 'I', updated_date = '{formatted_date}' where file_name = '{filename}';"""
        update_statements.append(statement)

    logger.info(f"Update statement created for staging table: {update_statements}")
    logger.info("Connection to MySQL connection")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("Connected to MySQL successfully.")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("There is something wrong in the execution process.")
    sys.exit()

input("Press Enter to terminate.")

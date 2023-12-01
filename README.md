# Store.Analysis-PySpark.project
### Description
In this project, we have 2 objectives:
  1. Find the sales person with the highest monthly sales and give him an incentive (1% of the total sales by him).
  2. Provide a complete table with customer data for future analysis.
### Architecture
![PySpark_Project_Archi drawio](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/82a9f9ea-c468-4783-a0e8-9c604af05f36)
### ER-Diagram
![ER-Diagram](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/bc33f7af-5f00-4f2e-bfeb-efa8d7e1c803)
### Technologies Used
#### Amazon S3: 
Amazon S3 is an object storage service that provides manufacturing scalability, data availability, security, and performance.
#### Apache Spark:
Apache Spark is a data processing framework that can quickly perform processing tasks on very large data sets, and can also distribute data processing tasks across multiple computers, either on its own or in tandem with other distributed computing tools. 
#### MySQL:
MySQL is a widely used relational database management system (RDBMS) which is both free and open-source.
### Project structure
```
my_project/
├── resources/
│   ├── __init__.py
│   ├── dev/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── qa/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── prod/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── sql_scripts/
│   │    └── table_scripts.sql
├── src/
│   ├── main/
│   │    ├── __init__.py
│   │    └── delete/
│   │    │      ├── aws_delete.py
│   │    │      └── local_file_delete.py
│   │    └── download/
│   │    │      └── aws_file_download.py
│   │    └── move/
│   │    │      └── move_files.py
│   │    └── read/
│   │    │      ├── aws_read.py
│   │    │      └── database_read.py
│   │    └── transformations/
│   │    │      └── jobs/
│   │    │      │     ├── customer_mart_sql_transform_write.py
│   │    │      │     ├── dimension_tables_join.py
│   │    │      │     ├── main.py
│   │    │      │     └──sales_mart_sql_transform_write.py
│   │    └── upload/
│   │    │      └── upload_to_s3.py
│   │    └── utility/
│   │    │      ├── encrypt_decrypt.py
│   │    │      ├── logging_config.py
│   │    │      ├── s3_client_object.py
│   │    │      ├── spark_session.py
│   │    │      └── my_sql_session.py
│   │    └── write/
│   │    │      ├── database_write.py
│   │    │      └── spark_writer.py
│   ├── test/
│   │    ├── extra_column_csv_generated_data.py
│   │    ├── generate_customer_table_data.py
│   │    ├── generate_datewise_sales_data.py
│   │    ├── generate_product_table_data.py
│   │    ├── generate_sales_team_data.py
│   │    ├── less_column_csv_generated_data.py
│   │    ├── sales_data_upload_s3.py
│   │    └── generate_csv_data.py
```

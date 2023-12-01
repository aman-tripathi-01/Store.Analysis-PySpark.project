# Store Analysis using PySpark

### Description
With this project, we have 2 objectives:
  1. Find the best sales person monthly and give him/her/them an incentive. (1% of their monthly sale)
  2. Prepare a table which has the necessary customer data for further analysis.

### Project Structure
```plaintext
my_project/
├── docs/
│   └── readme.md
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
│   │    └── generate_csv_data.py
│   │    └── generate_customer_table_data.py
│   │    └── generate_datewise_sales_data.py
│   │    └── generate_product_table_data.py
│   │    └── generate_sales_team_data.py
│   │    └── less_column_csv_generated_data.py
│   │    └── sales_data_upload_s3.py
```


### Project Architecture
![Project Architecture](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/26c0ac62-ff7f-49bd-8a87-2c41e445b4a2)

### Database ER Diagram
![Architecture](C:\Users\nikita\Documents\data_engineering\pythonProject\youtube_project\docs\database_schema.drawio.png)

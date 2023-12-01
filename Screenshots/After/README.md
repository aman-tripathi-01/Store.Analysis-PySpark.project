### Staging Table:
If the previous run failed:
  ![Screenshot 2023-12-01 223845](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/00d4ff69-d167-4322-ba2a-45180cdd0410)
If the previous run was successful:
  ![Screenshot 2023-12-01 232808](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/32c7f521-56f3-4bdd-8c45-4bf361f96c8e)

### AWS S3:
1. Customer Data Mart: This has the final processed customer data for further analysis.
    ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/f7137212-ab6b-434b-a63a-de426b1e0659)
2. Sales Data Error: The sales files has less columns / incorrect file extensions.
    ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/d2ced080-5ef5-4d2f-9f2f-2929b41fb3aa)
3. Sales Data Mart: This has the final processed sales people data along with details of the sales person who got incentives in parquet format.
    ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/3e38ae78-d44b-4a88-aef1-eed2aac8d2b9)
4. Sales Data Processed: The sales files after processing will be moved here.
    ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/b7c4c7b8-c942-4dfc-ac02-b96b665dd20a)
5. Sales Data: The input sales files after processing.
   ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/1d18adc1-6f88-4656-a1ec-c99ced695edb)
6. Sales Partitioned Data: The processsed sales people data partitioned by month and store_id and then saved in parquet format.
    ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/b262effa-e4aa-4b75-9a23-ba18a62fde69)
    ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/65b05c17-d55f-4314-a7af-8aba442b19cf)


### MySQL:
Customers Data Mart:

  ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/88e6d0e6-2b78-4010-8ecd-a75faed56e59)
  ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/99ea523f-bb14-44cb-9b27-b48328f3148d)

Sales Team Data Mart:

  ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/17a59357-e312-4943-9b9e-67619f8cc5d0)
  ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/abeda56c-adac-46e3-951b-cbe464721ffe)

Staging Table:
  ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/271187a2-9fa4-4662-be3e-b4fcc8a72565)

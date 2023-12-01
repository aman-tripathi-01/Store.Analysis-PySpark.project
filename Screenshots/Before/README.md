### AWS S3:
1. Customer Data Mart: This will have the final processed customer data for further analysis.
   ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/41d7ce64-d28c-42e0-ba7f-794ff1078607)
2. Sales Data Error: The sales files which have less columns / are of incorrect file extension will be moved here.
   ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/67f0e783-6baf-4e73-955b-9e81b0e653da)
3. Sales Data Mart: This will have the final processed sales people data with details of the sales person with incentives in parquet format.
   ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/a95ae0f1-8324-41f4-9b9f-beb13d932dd1)
4. Sales Data Processed: The sales files after processing will be moved here.
   ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/a4cf3d2b-db2b-48cc-98e8-801062718f17)
5. Sales Data: The input sales files.
   ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/9926e576-69d9-448e-8bc7-1e882e0b9820)
6. Sales Partitioned Data: The processsed sales people data will be partitioned by month and store_id and then saved here in parquet format.
   ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/14f8020c-fad6-4c3a-be8b-4e7a059ce146)

Uploaded Sales Data to S3:
![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/46bb17c0-3a96-48cd-913c-9c981ef2bc28)
![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/cc86700e-8bc6-4611-932f-06fa8771ef62)

### MySQL:
1. Customers Data Mart:
   
   ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/09f8593c-83ef-4c12-a1bf-e9426cdc5bdf)
2. Sales Team Data Mart:

   ![image](https://github.com/aman-tripathi-01/Store.Analysis-PySpark.project/assets/31034814/e50ca844-0bd8-4abe-882f-1b0c8bd0e8e5)

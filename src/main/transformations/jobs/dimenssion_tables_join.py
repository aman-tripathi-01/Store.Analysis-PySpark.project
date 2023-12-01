from pyspark.sql.functions import *
from src.main.utility.logging_config import *


# enriching the data from different tables


def dimensions_table_join(
    final_df_to_process, customer_table_df, store_table_df, sales_team_table_df
):
    # 1st - Join
    logger.info("Joining the final_df_to_process with customer_table_df ")
    s3_customer_df_join = (
        final_df_to_process.join(
            customer_table_df,
            ["customer_id"],
            "inner",
        )
        .withColumnRenamed("first_name", "customer_first_name")
        .withColumnRenamed("last_name", "customer_last_name")
        .withColumnRenamed("c_address", "customer_address")
        .withColumnRenamed("pincode", "customer_pincode")
        .drop(
            "product_name",
            "price",
            "quantity",
            "additional_column",
            "customer_joining_date",
        )
    )
    print(s3_customer_df_join.printSchema())

    logger.info("Joining the s3_customer_df_join with sales_team_table_df ")
    s3_customer_sales_df_join = (
        s3_customer_df_join.join(
            sales_team_table_df,
            sales_team_table_df["id"] == s3_customer_df_join["sales_person_id"],
            "inner",
        )
        .withColumnRenamed("id", "sales_person_ids")
        .withColumnRenamed("last_name", "sales_person_last_name")
        .withColumnRenamed("first_name", "sales_person_first_name")
        .withColumnRenamed("st_address", "sales_person_address")
        .withColumnRenamed("pincode", "sales_person_pincode")
        .drop(
            "sales_person_ids",
            "joining_date",
        )
    )
    print(s3_customer_sales_df_join.printSchema())

    logger.info("Joining the s3_customer_sales_df_join with store_table_df")
    s3_customer_sales_store_df_join = (
        s3_customer_sales_df_join.join(
            store_table_df,
            store_table_df.id == s3_customer_sales_df_join.store_id,
            "inner",
        )
        .withColumnRenamed("address", "store_address")
        .drop(
            "id",
        )
    )
    print(s3_customer_sales_store_df_join.printSchema())

    return s3_customer_sales_store_df_join

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.write.database_write import DatabaseWriter


# calculation for customer mart
def customer_mart_calculation_table_write(final_customer_data_mart_df):
    window = Window.partitionBy("customer_id", "purchase_month")
    final_customer_data_mart = (
        final_customer_data_mart_df.withColumn(
            "purchase_month", substring(col("sales_date"), 1, 7)
        )
        .withColumn(
            "total_purchase_every_month_by_each_customer",
            sum("total_cost").over(window),
        )
        .select(
            "customer_id",
            concat(
                col("customer_first_name"), lit(" "), col("customer_last_name")
            ).alias("full_name"),
            "customer_address",
            "phone_number",
            "purchase_month",
            col("total_purchase_every_month_by_each_customer").alias("total_purchase"),
        )
        .distinct()
    )
    print(final_customer_data_mart.show(5))
    print(final_customer_data_mart.count())

    # Write the Data into MySQL customers_data_mart table
    statement = f"""truncate table {config.customer_data_mart_table}"""
    connection = get_mysql_connection()
    cursor = connection.cursor()
    cursor.execute(statement)
    connection.commit()
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(final_customer_data_mart, config.customer_data_mart_table)
    cursor.close()
    connection.close()
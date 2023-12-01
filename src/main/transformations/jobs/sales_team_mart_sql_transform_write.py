from pyspark.sql.window import *
from pyspark.sql.functions import *
from resources.dev import config
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.write.database_write import DatabaseWriter
from src.main.utility.logging_config import *


def sales_team_mart_calculation_table_write(final_sales_team_data_mart_df):
    window = Window.partitionBy("store_id", "sales_person_id", "sale_month")
    final_sales_team_data_mart = (
        final_sales_team_data_mart_df.withColumn(
            "sale_month", substring(col("sales_date"), 1, 7)
        )
        .withColumn("total_sales_monthly", sum(col("total_cost")).over(window))
        .select(
            "store_id",
            "sales_person_id",
            concat(
                col("sales_person_first_name"), lit(" "), col("sales_person_last_name")
            ).alias("full_name"),
            "sale_month",
            "total_sales_monthly",
        )
    )

    rank_window = Window.partitionBy("store_id", "sale_month").orderBy(
        col("total_sales_monthly").desc()
    )
    final_sales_team_data_mart_table = (
        final_sales_team_data_mart.withColumn("ranks", rank().over(rank_window))
        .withColumn(
            "incentive",
            when(col("ranks") == 1, col("total_sales_monthly") * 0.01).otherwise(
                lit(0)
            ),
        )
        .withColumn("incentive", round(col("incentive"), 2))
        .withColumn("total_sales", col("total_sales_monthly"))
        .select("store_id", "sales_person_id", "total_sales", "incentive")
    )
    print(final_sales_team_data_mart_table.show(5))
    print(final_sales_team_data_mart_table.count())

    logging.info("Writing data into sales_team_data mart table in SQL")
    # Write the Data into MySQL customers_data_mart table
    statement = f"""truncate table {config.sales_team_data_mart_table}"""
    connection = get_mysql_connection()
    cursor = connection.cursor()
    cursor.execute(statement)
    connection.commit()
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(
        final_sales_team_data_mart_table, config.sales_team_data_mart_table
    )
    cursor.close()
    connection.close()

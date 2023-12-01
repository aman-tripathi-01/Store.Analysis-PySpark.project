import mysql.connector
from src.main.utility.encrypt_decrypt import *
from resources.dev import config

def get_mysql_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user=config.sql_user,
        password=decrypt(config.sql_password),
        database=config.database_name
    )
    return connection

















# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",
#     database="manish"
# )
#
# # Check if the connection is successful
# if connection.is_connected():
#     print("Connected to MySQL database")
#
# cursor = connection.cursor()
#
# # Execute a SQL query
# query = "SELECT * FROM manish.testing"
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor
# cursor.close()
#
# connection.close()

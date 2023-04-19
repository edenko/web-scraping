import mysql.connector

def get_database_connection():
    connection = mysql.connector.connect(
        host="",
        user="",
        password="",
        database=""
    )
    return connection

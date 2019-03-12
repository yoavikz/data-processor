import sqlite3
from sqlite3 import Error


def create_connection(db_path):
    try:
        connection = sqlite3.connect(db_path)
        return connection
    except Error as e:
        print(("Error when trying to connect sql db in path {}").format(db_path))
        print(e)


def query(connection, query_text):
    cur = connection.cursor()
    cur.execute(query_text)
    rows = cur.fetchall()
    return rows




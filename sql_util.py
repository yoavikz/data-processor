import sqlite3
from sqlite3 import Error


# creates a connection to sqlite3 database in a given path
def create_connection(db_path):
    try:
        connection = sqlite3.connect(db_path)
        return connection
    except Error as e:
        print(("Error when trying to connect sql db in path {}").format(db_path))
        print(e)


#  gets a db connection and a query text and returns the query result
def query(connection, query_text):
    cur = connection.cursor()
    cur.execute(query_text)
    rows = cur.fetchall()
    return rows


# returns true a table exists in the db, otherwise false
def is_table_in_db(connection, name_of_table):
    tables_list = query(connection,
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format(name_of_table))
    return len(tables_list) > 0


# create a table if it doesnt already exist in the db
def create_table(connection, table_name, columns):
    if is_table_in_db(connection, table_name):
        pass
    else:
        query_text = "CREATE TABLE {} ({});".format(table_name, columns)
        cur = connection.cursor()
        cur.execute(query_text)


# insert values (if dict format) into to a table. if the primary key exists alreay - replace
def insert_or_replace(connection, table_name, values):
    keys_for_query = str(tuple([key for key in values.keys()]))
    values_for_query = str(tuple([value for value in values.values()]))
    query_text = "INSERT OR REPLACE INTO {} {} VALUES {};".format(table_name, keys_for_query, values_for_query)
    cur = connection.cursor()
    cur.execute(query_text)
    connection.commit()

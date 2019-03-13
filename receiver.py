import rabbitmq_connection
import sql_util
import json
import files_util


def main():
    print("Starting receiver activity")
    connection = rabbitmq_connection.establish_connection(rabbitmq_connection.RABBITMQ_PATH)
    channel = rabbitmq_connection.get_channel(connection, rabbitmq_connection.RABBITMQ_QUEUE_NAME)
    receive(channel)

def receive(channel):
    channel.basic_consume(callback,
                          queue=rabbitmq_connection.RABBITMQ_QUEUE_NAME,
                          no_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

def callback(ch, method, properties, body):
    print(("Received message: {}").format(json.loads(body)))
    call_query_logics(json.loads(body))

def call_query_logics(message):
    conn = sql_util.create_connection(message["db"])
    first_question(conn)
    # stam_method(conn)
    third_question(conn)
    # second_question(conn)

def third_question(conn, year, country):
    query_text = \
        """SELECT  billingcountry,title
        FROM invoices JOIN invoice_items ON Invoices.invoiceid = invoice_items.invoiceid
        Join tracks on tracks.trackid = invoice_items.trackid
        join albums  on tracks.albumid = albums.albumid
                GROUP BY BillingCountry,title;
        """

    print("==========================")
    print(sql_util.query(conn, query_text))

def stam_method(conn):
    query_text = \
        """SELECT  billingcountry,title
        FROM invoices JOIN invoice_items ON Invoices.invoiceid = invoice_items.invoiceid
        Join tracks on tracks.trackid = invoice_items.trackid
        join albums  on tracks.albumid = albums.albumid
                GROUP BY BillingCountry,title;
        """

    print("==========================")
    print (sql_util.query(conn, query_text))

def second_question(conn):
    list_of_countries = query_list_of_all_countries(conn)
    print(list_of_countries)
    country_vs_albums = {country:query_albums_purchased_in_country(conn, country) for country in list_of_countries}
    print(country_vs_albums)



def first_question(conn):
    purchases_per_country_query_output = query_purchase_count_per_country(conn)
    print(purchases_per_country_query_output)
    files_util.write_to_csv(purchases_per_country_query_output, "purchase_count_by_country")


def query_purchase_count_per_country(conn):
    query_text=\
        """SELECT BillingCountry, COUNT(BillingCountry) 
        FROM invoices 
        GROUP BY BillingCountry;
        """
    return sql_util.query(conn, query_text)


def query_list_of_all_countries(conn):
    query_text = "SELECT DISTINCT BillingCountry FROM invoices;"
    query_output = sql_util.query(conn, query_text)
    country_list = [country[0] for country in query_output]
    return country_list

def query_albums_purchased_in_country(conn,country):
    query_text = \
        """SELECT  *
        FROM invoices JOIN invoice_items ON Invoices.invoiceid = invoice_items.invoiceid
        GROUP BY BillingCountry;
        """

    print("==========================")
    print (sql_util.query(conn, query_text))
    if country.lower() == 'usa':
        return ["Black album", "white album"]
    else:
        return ["stam album", "stamama"]



if __name__ == '__main__':
    main()
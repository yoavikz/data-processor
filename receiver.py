import rabbitmq_connection
import sql_util
import json
import files_util

PURCHASES_COUNT_PER_COUNTRY_CONSTANT_NAME = "purchase_count_per_country"


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

    # FIRST TASK
    purchases_per_country_query_output = query_purchase_count_per_country(conn)
    # writing to csv file
    files_util.write_to_csv(purchases_per_country_query_output, PURCHASES_COUNT_PER_COUNTRY_CONSTANT_NAME)
    # writing to db
    sql_util.create_table(conn, PURCHASES_COUNT_PER_COUNTRY_CONSTANT_NAME,
                          "country varchar(255) PRIMARY KEY, number_of_purchases int")
    for value in purchases_per_country_query_output:
        sql_util.insert_or_replace(conn, PURCHASES_COUNT_PER_COUNTRY_CONSTANT_NAME,
                                   {"country": value[0], "number_of_purchases": value[1]})

    # SECOND TASK
    albums_purchased_per_country_dict = list_albums_purchased_per_country(conn)
    # writing to json file
    files_util.write_to_json(albums_purchased_per_country_dict, "albums_purchased_per_country")

    # THIRD TASK
    best_sell_album = best_selling_album_in_country_since_date(conn, message["year"], message["country"])
    best_sell_album_dict_data = {"album": best_sell_album[0][0], "number_of_sales": best_sell_album[0][1],
                    "year": best_sell_album[0][2], "country": best_sell_album[0][3]}
    # writing to xml file
    files_util.write_to_xml(best_sell_album_dict_data,
                            "best_selling_album_in_{}_since_{}".format(message["country"], message["year"]),
                            "best_seller")
    # writing to db
    sql_util.create_table(conn, "best_selling_album",
                          "album varchar(255) , number_of_sales int, year int,country varchar(255),"
                          " PRIMARY KEY (country, year)")
    sql_util.insert_or_replace(conn, "best_selling_album", best_sell_album_dict_data)


def list_albums_purchased_per_country(conn):
    list_of_countries = query_list_of_all_countries(conn)
    country_vs_albums = {country: query_albums_purchased_in_country(conn, country) for country in list_of_countries}
    return country_vs_albums


# task 1 -  returns the most sold album in a specific country since a given year
def best_selling_album_in_country_since_date(conn, year, country):
    query_text = \
        (""" SELECT title, MAX(NUM_OF_SALES), '{}', '{}' FROM (SELECT title, COUNT (invoiceid) AS NUM_OF_SALES 
        FROM (SELECT  billingcountry,title,Invoices.invoiceid,genres.name,strftime('%Y',Invoices.invoiceDate) AS album_time
        FROM invoices JOIN invoice_items ON Invoices.invoiceid = invoice_items.invoiceid
            JOIN tracks ON tracks.trackid = invoice_items.trackid
            JOIN albums  ON tracks.albumid = albums.albumid
        JOIN genres ON tracks.genreid = genres.genreid
        WHERE billingcountry='{}' AND genres.name = '{}' AND album_time > '{}')
         GROUP BY title);
        """).format(year, country, country, 'Rock', year)

    return sql_util.query(conn, query_text)


# This method returns the number of ourchases per each country
def query_purchase_count_per_country(conn):
    query_text = \
        """SELECT BillingCountry, COUNT(BillingCountry) 
        FROM invoices 
        GROUP BY BillingCountry;
        """
    return sql_util.query(conn, query_text)


# This method returns a tuple of all distinct countries from which a purchase was made
def query_list_of_all_countries(conn):
    query_text = "SELECT DISTINCT BillingCountry FROM invoices;"
    query_output = sql_util.query(conn, query_text)
    country_list = (country[0] for country in query_output)
    return country_list


# This method gets a country name and returns all the albums purchased from an address in this country
def query_albums_purchased_in_country(conn, country):
    query_text = \
        """SELECT title FROM albums
        JOIN tracks  ON tracks.albumid = albums.albumid
        JOIN invoice_items ON invoice_items.trackid = invoice_items.trackid
        JOIN invoices ON Invoices.invoiceid = invoice_items.invoiceid
        WHERE invoices.billingcountry = '{}'
        GROUP BY title;
        """.format(country)

    return sql_util.query(conn, query_text)


if __name__ == '__main__':
    main()

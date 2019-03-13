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

    # first task
    purchases_per_country_query_output = query_purchase_count_per_country(conn)
    files_util.write_to_csv(purchases_per_country_query_output, "purchase_count_by_country")

    # second task
    albums_purchased_per_country_dict = list_albums_purchased_per_country(conn)
    #TODO add writing to json

    # third task
    best_selling_album_details = best_selling_album_in_country_since_date(conn, message["year"], message["country"])
    #TODO add writing to xml


def list_albums_purchased_per_country(conn):
    list_of_countries = query_list_of_all_countries(conn)
    country_vs_albums = {country: query_albums_purchased_in_country(conn, country) for country in list_of_countries}
    return country_vs_albums

#task 1 -  returns the most sold album in a specific country since a given year
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

#This method returns the number of ourchases per each country
def query_purchase_count_per_country(conn):
    query_text = \
        """SELECT BillingCountry, COUNT(BillingCountry) 
        FROM invoices 
        GROUP BY BillingCountry;
        """
    return sql_util.query(conn, query_text)


#This method returns a tuple of all distinct countries from which a purchase was made
def query_list_of_all_countries(conn):
    query_text = "SELECT DISTINCT BillingCountry FROM invoices;"
    query_output = sql_util.query(conn, query_text)
    country_list = (country[0] for country in query_output)
    return country_list


#This method gets a country name and returns all the albums purchased from an address in this country
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

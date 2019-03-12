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
    query_purchase_count_per_country(conn, message)

def query_purchase_count_per_country(conn, message):
    query_text=\
        """SELECT BillingCountry, COUNT(BillingCountry) 
        FROM invoices 
        GROUP BY BillingCountry;
        """
    query_output = sql_util.query(conn, query_text)
    files_util.write_to_csv(query_output, "purchase_count_by_country")
    print(query_output)

if __name__ == '__main__':
    main()
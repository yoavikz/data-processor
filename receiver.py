import rabbitmq_connection
import sql_util
import json


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
    stam_query(message)

def stam_query(message):
    conn = sql_util.create_connection(message["db"])
    rows = sql_util.query(conn, "select * from invoices limit 1;")
    print(rows)

if __name__ == '__main__':
    main()
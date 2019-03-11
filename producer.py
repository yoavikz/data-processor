import pika


#Consts
RABBITMQ_PATH = "localhost"
RABBITMQ_QUEUE_NAME = "queue"

def main():
    print("Starting producing activity")
    connection = establish_connection(RABBITMQ_PATH)
    channel = get_channel(connection, RABBITMQ_QUEUE_NAME)
    publish(channel, "key", "message body")
    close_connection(connection)
    print("Finished producing")


def establish_connection(rabbitmq_path):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_path))
    except Exception as e:
        print(("ERROR occurred when trying to connect to RabbitMQ in path {}").format(rabbitmq_path))
        print(e)
    print(("opened connection to rabbit :'{}'").format(connection))
    return connection

def close_connection(connection):
    try:
        connection.close()
    except Exception as e:
        print("ERROR occurred when trying to close connection to RabbitMQ")
        print(e)
    print(("closed connection to rabbit :'{}'").format(connection))

def get_channel(connection,queue_name):
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    return channel



def publish(channel, message_key, message_body):
    channel.basic_publish(exchange='',
                          routing_key=message_key,
                          body=message_body)
    print(("sent message: '{}'").format(message_body))


main()

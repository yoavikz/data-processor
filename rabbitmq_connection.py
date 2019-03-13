import pika

#Consts
RABBITMQ_PATH = "localhost"
RABBITMQ_QUEUE_NAME = "queue"

#Establishing connection to rabbitmq
def establish_connection(rabbitmq_path):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_path))
    except Exception as e:
        print(("ERROR occurred when trying to connect to RabbitMQ in path {}").format(rabbitmq_path))
        print(e)
    print(("opened connection to rabbit :'{}'").format(connection))
    return connection

#Closing a connection to rabbitmq
def close_connection(connection):
    try:
        connection.close()
    except Exception as e:
        print("ERROR occurred when trying to close connection to RabbitMQ")
        print(e)
    print(("closed connection to rabbit :'{}'").format(connection))

#declaring a queue and returning a channel
def get_channel(connection,queue_name):
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    return channel
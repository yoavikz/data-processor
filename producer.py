import rabbitmq_connection
import json

# Consts
DB_PATH = "C:\\sqlite\\db\\chinook.db"
COUNTRY = "BRAZIL"
YEAR = 1999

MESSAGE_BODY = {"db":DB_PATH, "country":COUNTRY, "year":YEAR}


def main():
    print("Starting producing activity")
    connection = rabbitmq_connection.establish_connection(rabbitmq_connection.RABBITMQ_PATH)
    channel = rabbitmq_connection.get_channel(connection, rabbitmq_connection.RABBITMQ_QUEUE_NAME)
    publish(channel, "key", MESSAGE_BODY)
    print (MESSAGE_BODY)
    rabbitmq_connection.close_connection(connection)
    print("Finished producing")


def publish(channel, message_key, message_body):
    channel.basic_publish(exchange='',
                          routing_key=rabbitmq_connection.RABBITMQ_QUEUE_NAME,
                          body=json.dumps(message_body))
    print(("sent message: '{}'").format(message_body))


if __name__ == '__main__':
    main()

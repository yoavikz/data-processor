import rabbitmq_connection
import json

# Consts - when running the program change only these properties
DB_PATH = "C:\\sqlite\\db\\chinook.db"
COUNTRY = "USA"
YEAR = 1999

MESSAGE_BODY = {"db": DB_PATH, "country": COUNTRY, "year": YEAR}


# Main method for producer module. creating connection and calling the publish method
def main():
    print("Starting producing activity")
    connection = rabbitmq_connection.establish_connection(rabbitmq_connection.RABBITMQ_PATH)
    channel = rabbitmq_connection.get_channel(connection, rabbitmq_connection.RABBITMQ_QUEUE_NAME)
    publish(channel, MESSAGE_BODY)
    rabbitmq_connection.close_connection(connection)


def publish(channel, message_body):
    channel.basic_publish(exchange='',
                          routing_key=rabbitmq_connection.RABBITMQ_QUEUE_NAME,
                          body=json.dumps(message_body))
    print(("sent message: '{}'").format(message_body))


if __name__ == '__main__':
    main()

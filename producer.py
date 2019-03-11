import connection_utils

#Consts


def main():
    print("Starting producing activity")
    connection = connection_utils.establish_connection(connection_utils.RABBITMQ_PATH)
    channel = connection_utils.get_channel(connection, connection_utils.RABBITMQ_QUEUE_NAME)
    publish(channel, "key", "message body")
    connection_utils.close_connection(connection)
    print("Finished producing")


def publish(channel, message_key, message_body):
    channel.basic_publish(exchange='',
                          routing_key=connection_utils.RABBITMQ_QUEUE_NAME,
                          body=message_body)
    print(("sent message: '{}'").format(message_body))


if __name__ == '__main__':
    main()



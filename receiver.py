import connection_utils

def main():
    print("Starting reveiver activity")
    connection = connection_utils.establish_connection(connection_utils.RABBITMQ_PATH)
    channel = connection_utils.get_channel(connection, connection_utils.RABBITMQ_QUEUE_NAME)

    channel.basic_consume(callback,
                          queue=connection_utils.RABBITMQ_QUEUE_NAME,
                          no_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def callback(ch, method, properties, body):
    print(("Received message: {}").format(body))

if __name__ == '__main__':
    main()
import rabbitmq_connection
import json
import sys
import ast

DEFAULT_MESSAGE = {"db": "C:\\sqlite\\db\\chinook.db", "country": "USA", "year": 1999}


# Main method for producer module. creating connection and calling the publish method to send message to rabbitmq
def main():
    print("Starting producing activity")
    message_body = get_message_from_command_line()
    connection = rabbitmq_connection.establish_connection(rabbitmq_connection.RABBITMQ_PATH)
    channel = rabbitmq_connection.get_channel(connection, rabbitmq_connection.RABBITMQ_QUEUE_NAME)
    publish(channel, message_body)
    rabbitmq_connection.close_connection(connection)


# sending the message to the queue
def publish(channel, message_body):
    channel.basic_publish(exchange='',
                          routing_key=rabbitmq_connection.RABBITMQ_QUEUE_NAME,
                          body=json.dumps(message_body))
    print(("sent message: '{}'").format(message_body))


# parsing json-format argument with message parameters. using default values if arg doesnt exist
def get_message_from_command_line():
    message = DEFAULT_MESSAGE
    if len(sys.argv) == 2:
        data = ast.literal_eval(sys.argv[1])
        for parameter in data:
            message[parameter] = data[parameter]
    return message


if __name__ == '__main__':
    main()

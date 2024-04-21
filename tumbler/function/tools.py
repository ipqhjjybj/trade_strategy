# encoding: UTF-8

import pika


def queue_delete(queue_name, connection_name='localhost'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(connection_name))
    channel = connection.channel()

    ret = channel.queue_delete(queue=queue_name)
    connection.close()
    return ret

import os
import pika


RABBIT_HOST = os.getenv('RABBIT_HOST', 'localhost')

class Rabbit:
    def __init__(self, queue_name):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def consume(self, func):
        self.channel.basic_consume(func,
                            queue=self.queue_name,
                            no_ack=True
                        )

    def start_consuming(self):
        self.channel.start_consuming()

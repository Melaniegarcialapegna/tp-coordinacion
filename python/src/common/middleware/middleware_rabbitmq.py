import pika
import random
import string
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)
class MessageMiddlewareRabbitMQ:
    PIKA_DISCONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ConnectionClosed,
    pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.StreamLostError,
)
    """
    Abstract class that implements the common functionality of the MessageMiddlewareQueue and MessageMiddlewareExchange classes for RabbitMQ
    """
    def __init__(self, host):
        # Establish a blocking connection to RabbitMQ server
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))

        self.channel = self.connection.channel()

        self.consuming = False

    def send(self, message):
        raise NotImplementedError("send method must be implemented in subclasses")

    def start_consuming(self, on_message_callback):
        # Transform the format of pika to the format of the middleware
        def _on_message_callback_internal(channel,method,propierties,body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag)

            # Who uses the middleware just need to call ack or nack
            # "abstracting" the details of the middleware
            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=_on_message_callback_internal)
            self.consuming = True

            self.channel.start_consuming()

        except self.PIKA_DISCONNECTION_ERRORS as error:
            raise MessageMiddlewareDisconnectedError(str(error))
        
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareMessageError(str(error))

        finally:
            # Warranty that always the consuming flag is going to
            # be false when the consuming finish or in case of error
            self.consuming = False

    def stop_consuming(self):
        if not self.consuming:
            return
        
        try: 
            self.channel.stop_consuming()

        except self.PIKA_DISCONNECTION_ERRORS as error:
            raise MessageMiddlewareDisconnectedError(str(error))

    def close(self):
        try:
            self.connection.close()

        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareCloseError(str(error))


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareRabbitMQ,MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        super().__init__(host)
        self.queue_name = queue_name

        # Declare a queue with the specified name
        self.channel.queue_declare(queue=self.queue_name,durable=True)

    def send(self, message):
        try:
            # In routing key goes the name of the queue to which the message will be sent
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

        except self.PIKA_DISCONNECTION_ERRORS as error:
            raise MessageMiddlewareDisconnectedError(str(error))
        
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareMessageError(str(error))    
        
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareRabbitMQ,MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        # Declare an exchange of type direct, it will be directed the messages to the queue with the same routing key as it
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        # Just for consumer
        self.queue_name = None

    def send(self, message):
        try:
            # Publish the message to all the routing keys
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)

        except self.PIKA_DISCONNECTION_ERRORS as error:
            raise MessageMiddlewareDisconnectedError(str(error))
        
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareMessageError(str(error))

    def start_consuming(self, on_message_callback):
        if self.queue_name is None:
            self._prepare_queue()

        super().start_consuming(on_message_callback)

    def _prepare_queue(self):
        # If is the firt time that the consumer is going to consume, it needs to create a anonimus queue and bind it to the exchange
        result = self.channel.queue_declare(queue='', durable=True, exclusive=True)
        self.queue_name = result.method.queue

        # Bind the queue to the exchange with all the routing keys
        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)
import os
import logging
import heapq
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_fruits = {} # {client_id: {fruit: FruitItem}}
        self.client_eof_count = {}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_exchange.stop_consuming()


    def _process_data(self,client_id , fruit, amount):
        logging.info("Processing data message")
        client_fruit_top = self.client_fruits.setdefault(client_id,{}) # return a reference of the dict ~ O(1)
        new_fruit_item = fruit_item.FruitItem(fruit, amount)

        if fruit in client_fruit_top: # O(1)
            client_fruit_top[fruit] = client_fruit_top[fruit] + new_fruit_item
        else:
            client_fruit_top[fruit] = new_fruit_item


    def _process_eof(self,client_id_eof):
        logging.info("Received EOF")

        # Wait until all instances of SumFilter send an EOF for the same client before calculating the top fruits for that client
        eof_count = self.client_eof_count.get(client_id_eof, 0) + 1
        self.client_eof_count[client_id_eof] = eof_count

        if eof_count < SUM_AMOUNT:
            logging.info(f"Waiting for {SUM_AMOUNT - eof_count} restant EOF messages for client {client_id_eof}")
            return

        client_fruits = self.client_fruits.pop(client_id_eof,{})
        fruit_top_size = heapq.nlargest(TOP_SIZE, client_fruits.values()) # O(n log TOP_SIZE)

        fruit_top = []
        for item in fruit_top_size:
            fruit_top.append((item.fruit,item.amount))

        self.output_queue.send(message_protocol.internal.serialize([client_id_eof, fruit_top]))

        self.client_eof_count.pop(client_id_eof,None)


    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)

        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()


    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            logging.info("Closing connections")
            self.input_exchange.close()
            self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

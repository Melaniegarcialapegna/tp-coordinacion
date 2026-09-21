import os
import logging
import heapq

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

    def _process_data(self,client_id , fruit, amount):
        logging.info("Processing data message")
        client_fruit_top = self.client_fruits.setdefault(client_id,{}) # return a reference of the dict
        new_fruit_item = fruit_item.FruitItem(fruit, amount)

        if fruit in client_fruit_top:
            client_fruit_top[fruit] = client_fruit_top[fruit] + new_fruit_item
        else:
            client_fruit_top[fruit] = new_fruit_item


    def _process_eof(self,client_id_eof):
        logging.info("Received EOF")
        client_fruits = self.client_fruits.pop(client_id_eof)
        fruit_top_size = heapq.nlargest(TOP_SIZE, client_fruits.values()) # O(n log TOP_SIZE)

        fruit_top = []
        for item in fruit_top_size:
            fruit_top.append(((client_id_eof, item.fruit), item.amount))

        self.output_queue.send(message_protocol.internal.serialize(fruit_top))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)

        if len(fields) == 2:
            [(client_id, fruit), amount] = fields
            self._process_data(client_id, fruit, amount)
        else:
            [client_id] = fields
            self._process_eof(client_id)
        ack()


    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

import os
import logging
import heapq

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.items_by_client = {} # {client_id:[FruitItem,..]}
        self.client_partials_top_count = {} 


    def _process_partial_top(self, client_id, fruit_top):
        logging.info(f"Received partial top for client {client_id}")

        items = self.items_by_client.setdefault(client_id,[])
        for fruit, amount in fruit_top:
            items.append(fruit_item.FruitItem(fruit, amount))

        partials_count = self.client_partials_top_count.get(client_id, 0) + 1
        self.client_partials_top_count[client_id] = partials_count

        if partials_count < SUM_AMOUNT:
            logging.info(f"Waiting for {SUM_AMOUNT - partials_count} restant partial top messages for client {client_id}")
            return

        logging.info(f"Merging final top for client {client_id}")
        final_top_items = heapq.nlargest(TOP_SIZE, items) # O(n log TOP_SIZE)
        final_top = []
        for item in final_top_items:
            final_top.append((item.fruit,item.amount))

        self.output_queue.send(message_protocol.internal.serialize([client_id, final_top]))

        self.items_by_client.pop(client_id,None)
        self.client_partials_top_count.pop(client_id,None)


    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        [client_id, fruit_top] = fields

        self._process_partial_top(client_id, fruit_top)
        ack()


    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

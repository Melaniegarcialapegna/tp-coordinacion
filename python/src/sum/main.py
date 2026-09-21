import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.clients_amount_by_fruit = {} # {client_id: {fruit: FruitItem}}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        client_fruits = self.clients_amount_by_fruit.setdefault(client_id,{}) # return a reference of the dict ~ O(1)
        new_fruit_item = fruit_item.FruitItem(fruit, int(amount))

        if fruit in client_fruits: # O(1)
            client_fruits[fruit] = client_fruits[fruit] + new_fruit_item
        else:
            client_fruits[fruit] = new_fruit_item

    def _process_eof(self, client_id_eof):
        logging.info(f"Broadcasting data messages")
        client_fruits = self.clients_amount_by_fruit.pop(client_id_eof)

        for item in client_fruits.values():
            message = message_protocol.internal.serialize([client_id_eof, item.fruit, item.amount])
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message)

        logging.info(f"Broadcasting EOF message")
        eof_message = message_protocol.internal.serialize([client_id_eof])
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(eof_message)


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

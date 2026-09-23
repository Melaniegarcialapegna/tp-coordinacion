import os
import logging
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
EOF_ROUTING_KEY = "EOF_ROUTING_KEY"
MSG_CLIENT_EOF = "CLIENT_EOF"
MSG_CLIENT_COUNT = "CLIENT_COUNT"

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.control_eof_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, SUM_CONTROL_EXCHANGE, [EOF_ROUTING_KEY])

        self.control_eof_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE,[EOF_ROUTING_KEY])

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_clients_and_fruit = {} # {client_id: {fruit: FruitItem}}
        self.amounts_lock = threading.Lock() 

        self.items_processed_by_clients_before_eof = {} 

        self.expected_total_by_clients = {} 
        self.confirmed_counts_by_clients = {}


    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        client_fruits = self.amount_by_clients_and_fruit.setdefault(client_id,{}) # return a reference of the dict ~ O(1)
        new_fruit_item = fruit_item.FruitItem(fruit, int(amount))

        if fruit in client_fruits: # O(1)
            client_fruits[fruit] = client_fruits[fruit] + new_fruit_item
        else:
            client_fruits[fruit] = new_fruit_item

        if client_id not in self.expected_total_by_clients:
            self.items_processed_by_clients_before_eof[client_id] = self.items_processed_by_clients_before_eof.get(client_id,0) + 1
        else:
            self._report_items(client_id, 1)


    def _report_items(self, client_id, count):
        message = message_protocol.internal.serialize([MSG_CLIENT_COUNT, client_id, count])
        self.control_eof_publisher.send(message)
            

    def _broadcast_eof(self, client_id_eof, total_fruits):
        logging.info(f"Client {client_id_eof} finished sending data: total_records={total_fruits}. Publishing EOF message to control exchange")
        self.control_eof_publisher.send(message_protocol.internal.serialize([MSG_CLIENT_EOF, client_id_eof,total_fruits]))


    def aggregation_index_for(self, fruit):
        digest = hashlib.md5(fruit.encode("utf-8")).digest()
        return int.from_bytes(digest[:4], "big") % AGGREGATION_AMOUNT


    def _handle_coordination_message(self, message_type, client_id, count):
        if message_type == MSG_CLIENT_EOF:
            logging.info("Received EOF message from client {client_id} with total count {count}")
            self.expected_total_by_clients[client_id] = count

            process_items_count = self.items_processed_by_clients_before_eof.get(client_id,0)
            if process_items_count > 0:
                self._report_items(client_id, process_items_count)

            if self._validate_client_count(client_id):
                self._process_eof(client_id)

        elif message_type == MSG_CLIENT_COUNT:
            logging.info("Received count message from client {client_id} with count {count}")
            self.confirmed_counts_by_clients[client_id] = self.confirmed_counts_by_clients.get(client_id,0) + count

            if self._validate_client_count(client_id):
                self._process_eof(client_id)


    def _validate_client_count(self, client_id):
        confirmed_count = self.confirmed_counts_by_clients.get(client_id,0)  

        total_expected_count = self.expected_total_by_clients.get(client_id,0)

        return client_id in self.expected_total_by_clients and confirmed_count == total_expected_count
    

    def _process_eof(self, client_id_eof):
        logging.info("Broadcasting data messages")
        client_fruits = self.amount_by_clients_and_fruit.pop(client_id_eof,{})

        for item in client_fruits.values():
            message = message_protocol.internal.serialize([client_id_eof, item.fruit, item.amount])
            target_aggregation_index = self.aggregation_index_for(item.fruit)
            self.data_output_exchanges[target_aggregation_index].send(message)

        logging.info(f"Broadcasting EOF message")
        eof_message = message_protocol.internal.serialize([client_id_eof])
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(eof_message)

        self.confirmed_counts_by_clients.pop(client_id_eof, None)
        self.expected_total_by_clients.pop(client_id_eof, None)


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if len(fields) == 3:
            with self.amounts_lock:
                self._process_data(*fields)
        else:
            self._broadcast_eof(*fields)
        ack()


    def process_coordination_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        
        with self.amounts_lock:
            self._handle_coordination_message(*fields)
        ack()

    def start(self):
        threading.Thread(target= lambda: self.control_eof_consumer.start_consuming(self.process_coordination_message),daemon=True).start()

        self.input_queue.start_consuming(self.process_data_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().hex #32 aleatory characters
    
    def serialize_data_message(self, message):
        """
        Serialize the message adding a client id
        """
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        """
        Serialize the EOF message adding a client id to know which client sent the EOF message.
        """
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        """
        Deserialize the message and check if it is for this client.
        In case it is not for this client, returns None, otherwise returns the fruit and amount.
        """
        fields = message_protocol.internal.deserialize(message)
        [id_client,fruit_top] = fields

        if id_client != self.client_id:
            return None
        
        return fruit_top

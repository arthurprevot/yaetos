"""Helper functions for kafka.
"""

import json
import jsonschema
import kafka
import requests
from yaetos.logger import setup_logging
logger = setup_logging('Kafka_push')


class KafkaProducer(object):

    def __init__(self, broker_address, topic, send_timeout, check_schema=False, schema_uri=None, connect_kafka=True):
        # TODO: add schema validation to only do message validation in the later stage (send()) to avoid validating schema for every record.
        self.__send_timeout = send_timeout
        self.__TOPIC = topic
        self.check_schema = check_schema
        if check_schema:
            self.schema_init(schema_uri)
        if connect_kafka:
            self.__producer = kafka.KafkaProducer(bootstrap_servers=[broker_address])
            logger.info("Producer connection started")

    def schema_init(self, schema_uri):
        self.__SCHEMA_URI = schema_uri
        response = requests.get(self.__SCHEMA_URI)
        schema = response.json()
        logger.info("Producer schema uri to pull schema data: {}".format(schema_uri))
        logger.info("Producer schema data pulled from schema service: {}.".format(schema))
        schema["$schema"] = self.__SCHEMA_URI # DIRTY FIX
        self.__schema = schema

    def build_message(self, **rec):
        """To be overriden by parent class"""
        raise NotImplementedError

    def send(self, rec, **kwargs):
        message = self.build_message(rec, **kwargs)  # TODO: remove this step from this function, or rename send() to build_and_send()

        to_send = True
        if self.check_schema:
            to_send = self.validate(message)

        # Actual send part
        if to_send:
            msg_in = bytes(bytearray(json.dumps(message), "UTF-8"))
            msg_out = self.__producer.send( self.__TOPIC, msg_in )

            # Post send
            msg_out.get(timeout=self.__send_timeout)
            return msg_out

    def validate(self, message):
        try:
            jsonschema.validate(instance=message, schema=self.__schema)
            return True
        except jsonschema.exceptions.ValidationError, e:
            logger.error(u"Validation error pre kafka send: {}.".format(str(e)))
            return False

    def flush(self):
        self.__producer.flush()

    def close(self):
        self.__producer.close()
        logger.info("Producer connection closed")

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        self.close()


if __name__ == '__main__':
    pass

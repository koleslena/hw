import pika
import time
import json


_QUEUE = 'rabbitmq'
_HB = 600
_ATTEMPS = 10

_FEATURES = 'features'
_Y_TRUE = 'y_true'
_Y_PRED = 'y_pred'

class MQ:
    def __init__(self):
        self.__channel = None
        self.__connection = None

    def __get_connection(self):
        if self.__connection and self.__connection.is_open:
            return self.__connection
        
        for attempt in range(_ATTEMPS):
            try:
                self.__connection = pika.BlockingConnection(pika.ConnectionParameters(_QUEUE, heartbeat=_HB))
                
                print("Connected")
                return self.__connection
            
            except Exception as e:
                print(f"Error connection. Retry in 5 sec...")
                time.sleep(5)

    def declare_channel(self, q):
        try:
            connection = self.__get_connection()
            self.__channel = connection.channel()
            self.__channel.queue_declare(queue=q, durable=True)
        except Exception as e:
            print(f"Error create channel {e}")
            if self.__channel and self.__channel.is_open:
                self.__channel.close()
            if connection and connection.is_open:
                connection.close()

    def create_consumer(self, q, cb):
        try:
            connection = self.__get_connection()
            self.__channel = connection.channel()
                    
            self.__channel.queue_declare(queue=q, durable=True)
            self.__channel.basic_consume(queue=q, on_message_callback=cb, auto_ack=True)
        except Exception as e:
            print(f"Error create channel {e}")
            if self.__channel and self.__channel.is_open:
                self.__channel.close()
            if connection and connection.is_open:
                connection.close()

    def _send_msg(self, msg, ch):
        try:
            connection = self.__get_connection()
            self.__channel = connection.channel()
                    
            self.__channel.basic_publish(
                                exchange='',
                                routing_key=ch,
                                body=json.dumps(msg)
                            )
        except Exception as e:
            print(f"Error send message {e}")
            if self.__channel and self.__channel.is_open:
                self.__channel.close()
            if connection and connection.is_open:
                connection.close()

    def consume(self):
        try:
            if self.__channel is None or self.__channel.is_closed:
                connection = self.__get_connection()
                self.__channel = connection.channel()

            if self.__channel and self.__channel.is_open:
                print('Consuming starts')
                self.__channel.start_consuming()
                
        except KeyboardInterrupt:
            print('Consuming stoped')
        except Exception:
            raise Exception('Error consuming')
        finally:
            self.__channel.stop_consuming()
            if self.__channel and self.__channel.is_open:
                self.__channel.close()
            if connection and connection.is_open:
                connection.close()


class FeatureMQ(MQ):
    def __init__(self):
        super().__init__()
        self.declare_channel(_FEATURES)
        self.declare_channel(_Y_TRUE)

    def send_true(self, msg):
        self._send_msg(msg, _Y_TRUE)

    def send_features(self, msg):
        self._send_msg(msg, _FEATURES)

class MetricsMQ(MQ):
    def __init__(self):
        super().__init__()

    def create_true_consumer(self, cb):
        self.create_consumer(_Y_TRUE, cb)

    def create_pred_consumer(self, cb):
        self.create_consumer(_Y_PRED, cb)

class ModelMQ(MQ):
    def __init__(self):
        super().__init__()
        self.declare_channel(_Y_PRED)

    def create_features_consumer(self, cb):
        self.create_consumer(_FEATURES, cb)       

    def send_pred(self, msg):
        self._send_msg(msg, _Y_PRED)

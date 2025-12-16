import sched
import time
import random
from datetime import datetime

from mq import FeatureMQ

_scheduler = sched.scheduler(time.time, time.sleep)

_mq = FeatureMQ()

_data = [{"features": {"Pclass": 1, "Age": 12.0, "Fare": 1000.0}, "y_true": 1}, 
         {"features": {"Pclass": 3, "Age": 80.0, "Fare": 0.1}, "y_true": 0}, 
         {"features": {"Pclass": 3, "Age": 90.0, "Fare": 10.0}, "y_true": 1}, 
         {"features": {"Pclass": 2, "Age": 21.0, "Fare": 10000.0}, "y_true": 1},]

def _send_messages():
    print("Run send features")
    try:
        message_id = datetime.timestamp(datetime.now())

        i = random.choice([0, 1, 2])

        message_feature = {
            'id': message_id,
            'body': _data[i]["features"]
        }
        _mq.send_features(message_feature)
        message_y_true = {
            'id': message_id,
            'body': _data[i]["y_true"]
        }
        _mq.send_true(message_y_true)
        
    except Exception as e:
        print(f"Send features error: {e}")
    finally:
        print("Run send features finished")


def schedule_next_event():
    _scheduler.enter(10, 1, _send_messages)
    _scheduler.run()

_scheduler.enter(2, 1, _send_messages)

while True:
    schedule_next_event()
import json
import os
import csv

from mq import MetricsMQ

os.makedirs('logs', exist_ok=True)

_METRICS = 'logs/metric_log.csv'
file_exists = os.path.exists(_METRICS)
if not file_exists:
    with open(_METRICS, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'y_true', 'y_pred', 'absolute_error'])

_msgs = {}

def __save_file(message_id):
    (_true, _pred) = _msgs[message_id]
    if _true is not None and _pred is not None:
        absolute_error = abs(_true - _pred)

        row = [message_id, _true, _pred, absolute_error]

        with open(_METRICS, 'a', newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(row)
        
        print(f"Saved row : {row}")

        del _msgs[message_id]

def __set_msg(message_id, y_true=None, y_pred=None):
    (_true, _pred) = _msgs[message_id] if message_id in _msgs else (None, None)
    _msgs[message_id] = (_true if y_true is None else y_true, _pred if y_pred is None else y_pred)
    __save_file(message_id)

def save_trues(ch, method, properties, body):
    print("Save true")
    if not body:
        return
        
    data = json.loads(body)
    message_id = data['id']
    y_true = data['body']
    print(f"Save true: {y_true}")
    __set_msg(message_id, y_true=y_true)

def save_preds(ch, method, properties, body):
    print("Save prediction")
    if not body:
        return
        
    data = json.loads(body)
    message_id = data['id']
    y_pred = data['body']
    print(f"Save prediction: {y_pred}")
    __set_msg(message_id, y_pred=y_pred)


_mq = MetricsMQ()
_mq.create_true_consumer(save_trues)
_mq.create_pred_consumer(save_preds)
_mq.consume()

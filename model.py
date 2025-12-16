import pandas as pd
import json
import pickle

from mq import ModelMQ

def __get_model():
    with open('model.pkl', 'rb') as f:
        model = pickle.load(f)
        return model

_model = __get_model()

def make_predict(ch, method, properties, body):
    print("Run make prediction")
    if not body:
        return
        
    data = json.loads(body)
    message_id = data['id']
    features = data['body']

    print(f"Run make prediction. Got features: {features}")

    data = pd.DataFrame({
        'Pclass': [features["Pclass"]],
        'Age': [features["Age"]],
        'Fare': [features["Fare"]]
    })

    predictions = _model.predict(data)
    prediction = predictions[0]
    
    print(f"Prediction: {prediction}")

    prediction_msg = {
        'id': message_id,
        'body': int(prediction)
    }
    _mq.send_pred(prediction_msg)
    print(f"Predictions sent {prediction}")



_mq = ModelMQ()
_mq.create_features_consumer(make_predict)
_mq.consume()

from kafka import KafkaProducer
import datetime
import time
import uuid

producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'])

userId = "Jonh_Santana"
topic = "problem3"

while True:
    uid = uuid.uuid4()
    now = datetime.datetime.now()
    message = bytes(f"{uid},{now},{userId}", 'utf-8')

    print(f'Sending message {message}')
    producer.send(topic ,value=message, key=message)

    time.sleep(1)

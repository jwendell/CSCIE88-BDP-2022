from kafka import KafkaConsumer

import sys

client_id = sys.argv[1] if len(sys.argv) > 1 else "default"

while True:
    consumer = KafkaConsumer(bootstrap_servers=[
                             'localhost:9092', 'localhost:9093', 'localhost:9094'], group_id='p3consumer', client_id=client_id, auto_offset_reset='latest')
    consumer.subscribe(['problem3'])

    for message in consumer:
        print(
            f"Message arrived. Offset: {message.offset}, Partition: {message.partition}, Key: {message.key.decode('utf-8')}, Value: {message.value.decode('utf-8')}")

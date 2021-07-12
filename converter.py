from kafka import KafkaConsumer # kafka-python
import json
import time

consumer = KafkaConsumer( 
     bootstrap_servers=['localhost:9092'],
     enable_auto_commit=True)

consumer.subscribe(['coords','temperature'])
# print(consumer.subscription())

while True:
    time.sleep(0.01)

    for message in consumer:

        message_val = message.value.decode("utf-8") 
        msg_json = json.loads(message_val)
        if message.topic == 'coords':
            print('x= ', msg_json["x"])
        if message.topic == 'temperature':
            print('t= ', msg_json["temperature"])





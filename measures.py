import time
import random
import math
from pykafka import KafkaClient # pykafka
import json

i = 1
j = 1
kafka_client = KafkaClient(hosts="localhost:9092")

kafka_topic = kafka_client.topics['coords']
kafka_producer_xyz = kafka_topic.get_sync_producer()

kafka_topic = kafka_client.topics['temperature']
kafka_producer_t = kafka_topic.get_sync_producer()

print("Generator loop init")

while True:

	x = random.gauss(5, 1)
	y = random.gauss(-7, 0.1) + i / 200
	z = random.gauss(20, 4) 
	temp = random.gauss(50, 4) - math.sqrt(215 / i)
	# pressure = random.gauss(100, 4) + 1.5 * j

	if random.random() > 0.5:
		msg = json.dumps({"x": x, "y": y, "z": z})
		kafka_producer_xyz.produce(msg.encode('ascii'))

	else:
		msg = json.dumps({"temperature": temp})
		kafka_producer_t.produce(msg.encode('ascii'))

	i += 0.5
	if j < 50:
		j += 0.5
		
	time.sleep(0.05)

	# print('Send: ' + msg)
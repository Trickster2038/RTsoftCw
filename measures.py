import time
import random
import math
from pykafka import KafkaClient
import json

i = 1
j = 1
kafka_client = KafkaClient(hosts="localhost:9092")

kafka_topic1 = kafka_client.topics['coords']
kafka_producer1 = kafka_topic1.get_sync_producer()

kafka_topic2 = kafka_client.topics['temperature']
kafka_producer2 = kafka_topic2.get_sync_producer()

while True:
	# print(random.random())\
	# print(random.gauss(5, 1))

	x = random.gauss(5, 1)
	y = random.gauss(10, 0.5)
	z = random.gauss(-7, 2)
	temp = random.gauss(50, 4) - math.sqrt(215 / i)
	pressure = random.gauss(100, 4) + 1.5 * j
	
	i += 0.5
	if j < 50:
		j += 0.5
	time.sleep(0.5)

	if random.random() > 0.5:
		msg = json.dumps({"x": x, "y": y, "z": z})
		kafka_producer1.produce(msg.encode('ascii'))

	else:
		msg = json.dumps({"temperature": temp})
		kafka_producer2.produce(msg.encode('ascii'))

	print("Send: " + msg)
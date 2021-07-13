import time
import random
import math
from pykafka import KafkaClient # pykafka
import json
from datetime import datetime

def upd_log_ok(log_file, msg):
	log_file.write(f"[{datetime.now()}] Message {msg} was created and sent.\n")

def upd_log_fail(log_file, msg):
	if msg:
		log_file.write(f"[{datetime.now()}] Message {msg} was created but not sent.\n")
	else:
		log_file.write(f"[{datetime.now()}] Message wasn't created so there is nothing to sent.\n")

i = 1
log_file = open("logs/emulator.log", "w")

kafka_client = KafkaClient(hosts="localhost:9092")

kafka_topic = kafka_client.topics['coords']
kafka_producer_xyz = kafka_topic.get_sync_producer()

kafka_topic = kafka_client.topics['temperature']
kafka_producer_t = kafka_topic.get_sync_producer()

kafka_topic = kafka_client.topics['electrical']
kafka_producer_e = kafka_topic.get_sync_producer()

kafka_topic = kafka_client.topics['generator']
kafka_producer_g = kafka_topic.get_sync_producer()

print("Generator loop init")

while True:

	to_send = random.randint(0, 3)

	if to_send == 0:
		vol = round(random.uniform(730,790), 3)
		cur = round(random.gauss(100,5), 3)
		res = 200 + round(random.random() - 0.5, 3)
		msg = json.dumps({"voltage": vol, "current": cur, "resistance": res})
		if kafka_producer_e.produce(msg.encode('ascii')):
			upd_log_ok(log_file, msg)
		else:
			upd_log_fail(log_file, msg)

	elif to_send == 1:
		x = round(random.gauss(5, 1), 3)
		y = round(random.gauss(-7, 0.1) + i / 200, 3)
		z = round(random.gauss(20, 4), 3)
		msg = json.dumps({"x": x, "y": y, "z": z})
		if kafka_producer_xyz.produce(msg.encode('ascii')):
			upd_log_ok(log_file, msg)
		else:
			upd_log_fail(log_file, msg)

	elif to_send == 2:
		temp = round(random.gauss(50, 4) - math.sqrt(215 / i), 3)
		press = round(random.normalvariate(100 + i / 10, 3), 3)
		hum = random.randint(40, 50)
		msg = json.dumps({"temperature": temp, "pressure": press, "humidity": hum})
		if kafka_producer_t.produce(msg.encode('ascii')):
			upd_log_ok(log_file, msg)
		else:
			upd_log_fail(log_file, msg)

	elif to_send == 3:
		freq = round(47 + random.random() * 5, 3)
		power = round(random.uniform(5.2, 5.8), 3)
		noise = random.randint(66, 80)
		msg = json.dumps({"frequency": freq, "power": power, "noise": noise})
		if kafka_producer_g.produce(msg.encode('ascii')):
			upd_log_ok(log_file, msg)
		else:
			upd_log_fail(log_file, msg)

	i += 0.5
		
	time.sleep(0.05)

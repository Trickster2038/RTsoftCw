from kafka import KafkaConsumer # kafka-python
import json
import time
from opcua import Server
from opcua.ua import VariantType

URL = "opc.tcp://0.0.0.0:4840"

server = Server()
server.set_endpoint(URL)

# print(dir(server))
# server.set_application_uri("urn:freeopcua:python:server")
# server.set_server_name("myOpcServer")

objects   = server.get_objects_node()
ns        = server.register_namespace("My metrics")
accelerometer = objects.add_object(ns, "accelerometer")    
x_metric = accelerometer.add_variable(ns, "x", 0.0, varianttype = VariantType.Double)
y_metric = accelerometer.add_variable(ns, "y", 0.0, varianttype = VariantType.Double)
z_metric = accelerometer.add_variable(ns, "z", 0.0, varianttype = VariantType.Double)
termometer = objects.add_object(ns, "termometer") 
t_metric = termometer.add_variable(ns, "temperature", 0.0, varianttype = VariantType.Double)
 
server.start()
     

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
            # print('x= ', msg_json["x"])
            x_metric.set_value(msg_json["x"], varianttype = VariantType.Double)
            y_metric.set_value(msg_json["y"], varianttype = VariantType.Double)
            z_metric.set_value(msg_json["z"], varianttype = VariantType.Double)

        if message.topic == 'temperature':
            # print('t= ', msg_json["temperature"])
            t_metric.set_value(msg_json["temperature"], varianttype = VariantType.Double)
            
        print("Get: ", message_val)






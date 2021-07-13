from kafka import KafkaConsumer # kafka-python
import json
import time
from opcua import Server
from opcua.ua import VariantType
from datetime import datetime

def upd_log(log_file, msg):
    log_file.write(f"[{datetime.now()}] Message {msg} was received and converted into OPC UA variables.\n")

log_file = open("logs/converter.log", "w")

URL = "opc.tcp://0.0.0.0:4840"

server = Server()
server.set_endpoint(URL)

objects   = server.get_objects_node()
ns        = server.register_namespace("My metrics")

accelerometer = objects.add_object(ns, "accelerometer")    
x_metric = accelerometer.add_variable(ns, "x", 0.0, varianttype = VariantType.Double)
y_metric = accelerometer.add_variable(ns, "y", 0.0, varianttype = VariantType.Double)
z_metric = accelerometer.add_variable(ns, "z", 0.0, varianttype = VariantType.Double)

termohygrometer = objects.add_object(ns, "thermohygrometer") 
t_metric = termohygrometer.add_variable(ns, "temperature", 0.0, varianttype = VariantType.Double)
p_metric = termohygrometer.add_variable(ns, "pressure", 0.0, varianttype = VariantType.Double)
h_metric = termohygrometer.add_variable(ns, "humidity", 0.0, varianttype = VariantType.Double)

multimeter = objects.add_object(ns, "multimeter") 
v_metric = multimeter.add_variable(ns, "voltage", 0.0, varianttype = VariantType.Double)
c_metric = multimeter.add_variable(ns, "current", 0.0, varianttype = VariantType.Double)
r_metric = multimeter.add_variable(ns, "resistance", 0.0, varianttype = VariantType.Double)

generator_sensor = objects.add_object(ns, "generator_sensor") 
f_metric = generator_sensor.add_variable(ns, "frequency", 0.0, varianttype = VariantType.Double)
po_metric = generator_sensor.add_variable(ns, "power", 0.0, varianttype = VariantType.Double)
n_metric = generator_sensor.add_variable(ns, "noise", 0.0, varianttype = VariantType.Double)

server.start()
     
consumer = KafkaConsumer( 
     bootstrap_servers=['localhost:9092'],
     enable_auto_commit=True)

consumer.subscribe(['coords','temperature','electrical','generator'])

print("Converter loop init")

while True:

    for message in consumer:

        message_val = message.value.decode("utf-8") 
        msg_json = json.loads(message_val)

        if message.topic == 'coords':
            x_metric.set_value(msg_json["x"], varianttype = VariantType.Double)
            y_metric.set_value(msg_json["y"], varianttype = VariantType.Double)
            z_metric.set_value(msg_json["z"], varianttype = VariantType.Double)
            upd_log(log_file, message_val)

        elif message.topic == 'temperature':
            t_metric.set_value(msg_json["temperature"], varianttype = VariantType.Double)
            p_metric.set_value(msg_json["pressure"], varianttype = VariantType.Double)
            h_metric.set_value(msg_json["humidity"], varianttype = VariantType.Double)
            upd_log(log_file, message_val)
            
        elif message.topic == 'electrical':
            v_metric.set_value(msg_json["voltage"], varianttype = VariantType.Double)
            c_metric.set_value(msg_json["current"], varianttype = VariantType.Double)
            r_metric.set_value(msg_json["resistance"], varianttype = VariantType.Double)
            upd_log(log_file, message_val)

        elif message.topic == 'generator':
            f_metric.set_value(msg_json["frequency"], varianttype = VariantType.Double)
            po_metric.set_value(msg_json["power"], varianttype = VariantType.Double)
            n_metric.set_value(msg_json["noise"], varianttype = VariantType.Double)
            upd_log(log_file, message_val)

    time.sleep(0.05)

import time
from opcua import Client
from influxdb import InfluxDBClient

db_client = InfluxDBClient(host='localhost', port=8086)
db_client.create_database('opcdata')
db_client.switch_database('opcdata')
fl = db_client.query('Delete FROM accel WHERE time > 0')
fl = db_client.query('Delete FROM temperature WHERE time > 0')

URL = "opc.tcp://localhost:4840"
 
if __name__ == "__main__":
    client = Client(URL)
    client.connect()
     
    xNode = client.get_node("ns=2;i=2") 
    yNode = client.get_node("ns=2;i=3")
    zNode = client.get_node("ns=2;i=4")

    tNode = client.get_node("ns=2;i=6")
     
    print("Client loop init")
    
    while True:
      # print("Client listen 1 sec tick")

      x = xNode.get_value()
      y = yNode.get_value()
      z = zNode.get_value()

      t = tNode.get_value()

      json_body = [
        {
        "measurement": "accel",
        "fields":{
            "x": x,
            "y": y,
            "z": z
            }
        }
         ]

      flag = db_client.write_points(json_body)

      json_body = [
        {
        "measurement": "temperature",
        "fields":{
            "t": t
            }
        }
         ]

      flag = db_client.write_points(json_body)
       
      time.sleep(0.05)
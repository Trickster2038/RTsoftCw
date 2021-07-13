import time
from opcua import Client
from influxdb import InfluxDBClient
from datetime import datetime

def upd_log_ok(log_file, values, headers):
  variables = ''
  for i in range(len(values)):
    to_add = "'" + headers[i] + "': " + str(values[i])
    variables += to_add
    if i != len(values) - 1:
      variables += ", "
    pass
  log_file.write(f"[{datetime.now()}] OPC UA variables {variables} were read and stored into InfluxDB.\n")

def upd_log_fail(log_file):
  log_file.write(f"[{datetime.now()}] Failed to store OPC UA variables into InfluxDB.\n")

log_file = open("logs/client.log", "w")

db_client = InfluxDBClient(host='localhost', port=8086)
db_client.create_database('opcdata')
db_client.switch_database('opcdata')
fl = db_client.query('Delete FROM accel WHERE time > 0')
fl = db_client.query('Delete FROM temperature WHERE time > 0')
fl = db_client.query('Delete FROM electrical WHERE time > 0')
fl = db_client.query('Delete FROM generator WHERE time > 0')

URL = "opc.tcp://localhost:4840"
 
if __name__ == "__main__":
    client = Client(URL)
    client.connect()
     
    xNode = client.get_node("ns=2;i=2") 
    yNode = client.get_node("ns=2;i=3")
    zNode = client.get_node("ns=2;i=4")

    tNode = client.get_node("ns=2;i=6")
    pNode = client.get_node("ns=2;i=7")
    hNode = client.get_node("ns=2;i=8")

    vNode = client.get_node("ns=2;i=10")
    cNode = client.get_node("ns=2;i=11")
    rNode = client.get_node("ns=2;i=12")

    fNode = client.get_node("ns=2;i=14")
    poNode = client.get_node("ns=2;i=15")
    nNode = client.get_node("ns=2;i=16")
     
    print("Client loop init")
    
    while True:

      x = xNode.get_value()
      y = yNode.get_value()
      z = zNode.get_value()

      t = tNode.get_value()
      p = pNode.get_value()
      h = hNode.get_value()

      v = vNode.get_value()
      c = cNode.get_value()
      r = rNode.get_value()

      f = fNode.get_value()
      po = poNode.get_value()
      n = nNode.get_value()

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

      if db_client.write_points(json_body):
        upd_log_ok(log_file, [x, y, z], ['x', 'y', 'z'])
      else:
        upd_log_fail(log_file)

      json_body = [
        {
        "measurement": "temperature",
        "fields":{
            "temp": t,
            "press": p,
            "hum": h
            }
        }
         ]

      if db_client.write_points(json_body):
        upd_log_ok(log_file, [t, p, h], ['temperature', 'pressuer', 'humidity'])
      else:
        upd_log_fail(log_file)

      json_body = [
        {
        "measurement": "electrical",
        "fields":{
            "volt": v,
            "curr": c,
            "res": r
            }
        }
         ]

      if db_client.write_points(json_body):
        upd_log_ok(log_file, [v, c, r], ['voltage', 'current', 'resistance'])
      else:
        upd_log_fail(log_file)

      json_body = [
        {
        "measurement": "generator",
        "fields":{
            "freq": f,
            "power": po,
            "noise": n
            }
        }
         ]

      if db_client.write_points(json_body):
        upd_log_ok(log_file, [f, po, n], ['frequency', 'power', 'noise'])
      else:
        upd_log_fail(log_file)
       
      time.sleep(0.05)

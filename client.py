import time
from opcua import Client
# from opcua import ua

URL = "opc.tcp://localhost:4840"
 
if __name__ == "__main__":
  client = Client(URL)
  client.connect()
   
  yNode = client.get_node("ns=2;i=3")
   
  while True:
    y = yNode.get_value()
     
    print("y= ", type(y))
    print("y= ", y)
    print("y= ", get_data_type_as_variant_type(y))
     
    time.sleep(1)
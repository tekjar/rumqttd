import paho.mqtt.client as mqtt
import time

BROKER = "localhost"
PORT = 1883
KEEP_ALIVE = 60

client = mqtt.Client()

client.connect(BROKER, PORT, KEEP_ALIVE)

for i in range(1000):
    client.publish("hello/mqtt", "hello mqtt", False)
    time.sleep(1)

client.loop_forever()
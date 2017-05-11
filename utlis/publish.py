import paho.mqtt.client as mqtt
import time

BROKER = "localhost"
PORT = 1883
KEEP_ALIVE = 10

client = mqtt.Client()

client.connect(BROKER, PORT, KEEP_ALIVE)

for i in range(700):
    client.publish("hello/+/mqtt", "hello mqtt", 1, False)
    print('{}. Publish'.format(i))
    # time.sleep(0.1)

client.loop_forever()

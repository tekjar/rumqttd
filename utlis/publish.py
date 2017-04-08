import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
KEEP_ALIVE = 5

client = mqtt.Client()

client.connect(BROKER, PORT, KEEP_ALIVE)

for i in range(100):
    client.publish("hello/mqtt", "hello mqtt 11111111111111111111111111", False)

client.loop_forever()
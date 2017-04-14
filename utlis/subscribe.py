import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
KEEP_ALIVE = 5

client = mqtt.Client()

client.connect(BROKER, PORT, KEEP_ALIVE)
client.subscribe("hello/mqtt", 2)

def on_message(client, userdata, message):
    print("%s %s" % (message.topic, message.payload))

client.on_message = on_message

client.loop_forever()
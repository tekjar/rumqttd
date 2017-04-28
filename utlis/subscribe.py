import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
KEEP_ALIVE = 30

client = mqtt.Client()

client.connect(BROKER, PORT, KEEP_ALIVE)
client.subscribe("hello/mqtt", 2)

def on_disconnect(client, userdata, rc):
    print("disconnet: ", client)

def on_message(client, userdata, message):
    print("%s %s" % (message.topic, message.payload))

client.on_message = on_message
client.on_disconnect = on_disconnect

client.loop_forever()
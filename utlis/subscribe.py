import paho.mqtt.client as mqtt
import time
import sys
import threading

BROKER = "localhost"
PORT = 1883
KEEP_ALIVE = 60
COUNTER = 0
START_TIME = time.time()
LOCK = threading.Lock()

client = mqtt.Client()

client.connect(BROKER, PORT, KEEP_ALIVE)
client.subscribe("hello/mqtt", 1)

def on_message(client, userdata, message):
    global COUNTER
    global START_TIME

    with LOCK:
        COUNTER = COUNTER + 1
        # print("{}.{}".format(COUNTER, message.topic))
        if COUNTER == 500:
            print('execution time = ', time.time() - START_TIME)
            COUNTER = 0
            START_TIME = time.time()

def on_disconnect(client, userdata, rc):
    print("disconnet: ", client)

client.on_message = on_message
client.on_disconnect = on_disconnect

client.loop_forever()
import paho.mqtt.client as mqtt
import time
import sys
import threading

BROKER = "localhost"
PORT = 1883
KEEP_ALIVE = 60
COUNTER = 0
START_TIME = time.time()
END_TIME = None
LOCK = threading.Lock()

def on_message(client, userdata, message):
    global COUNTER
    global START_TIME
    global END_TIME
    global LOCK

    with LOCK:
        COUNTER = COUNTER + 1
        # print("{}.{}".format(COUNTER, message.topic))
        if COUNTER > 490:
            END_TIME = time.time()

def benchmark():
    global START_TIME
    global END_TIME
    global COUNTER

    START_TIME = time.time()

    client1 = mqtt.Client()
    client1.connect(BROKER, PORT, KEEP_ALIVE)
    client1.loop_start()

    client2 = mqtt.Client()
    client2.connect(BROKER, PORT, KEEP_ALIVE)
    client2.loop_start()

    client1.on_message = on_message
    client1.subscribe("hello/mqtt", 1)

    for i in range(500):
        client2.publish("hello/mqtt", "hello mqtt", 1, False)
    
    time.sleep(3)
    client1.disconnect()
    client2.disconnect()
    execution_time = END_TIME - START_TIME
    print('execution time = {}'.format(execution_time))
    with LOCK:
        print('dropped = {}\n'.format(500 - COUNTER))
    return execution_time

times = []
for i in range(3):
    COUNTER = 0
    print('------------------- benchmark ' + str(i) + ' --------------------')
    t = benchmark()
    times.append(t)

print('\naverage time = {}'.format(sum(times)/len(times)))
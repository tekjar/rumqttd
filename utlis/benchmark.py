import paho.mqtt.client as mqtt
import time
import sys
import threading
import itertools

BROKER = "localhost"
PORT = 1883
KEEP_ALIVE = 60
COUNTER = itertools.count()
START_TIME = time.time()
END_TIME = None
LOCK = threading.Lock()
NUMBER_OF_PUBLISHES = 50000
WAIT_TIME = 100

def on_message(client, userdata, message):
    global COUNTER
    global START_TIME
    global END_TIME

    # print("{}.{}".format(COUNTER, message.topic))
    if COUNTER.next() > (NUMBER_OF_PUBLISHES - 10):
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

    for i in range(NUMBER_OF_PUBLISHES):
        client2.publish("hello/mqtt", "hello mqtt", 1, False)
    
    time.sleep(WAIT_TIME)
    client1.disconnect()
    client2.disconnect()
    execution_time = END_TIME - START_TIME
    print('execution time = {}'.format(execution_time))
    print('dropped = {}\n'.format(NUMBER_OF_PUBLISHES - COUNTER.next()))
    return execution_time

times = []
for i in range(3):
    COUNTER = itertools.count()
    print('------------------- benchmark ' + str(i) + ' --------------------')
    t = benchmark()
    times.append(t)

print('\naverage time = {}'.format(sum(times)/len(times)))
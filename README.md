## No more development here. Follow [this](https://github.com/bytebeamio/rumqtt) repo for progress


# rumqttd
High performance tokio based rust mqtt broker

# features

* MQTT 3.1.1
* QoS 0, 1, 2 Publishes & Subscribes
* Persistent Sessions
* Last Will
* Retained Messages
* Passes paho interoperability test suite

# todo

- [ ] username password authentication
- [ ] tls
- [ ] shared subscription
- [ ] grafana (or something similar) based dashboard plugin
- [ ] clustering

# benchmarks

Subscribe to a wildcard topic & publish 1 million qos1 messages (and wait for acks)
to the broker on local machine

model: MacBook Air (13-inch, Early 2014)
processor: 1.4 GHz Intel Core i5
memory: 4 GB 1600 MHz DDR3

**mosquitto (1.4.11)**

```
2017/07/23 22:42:59 time taken for publishes = 3m41.531635728s
load average 1 =  2.576171875
average system memory =  2417095749
2017/07/23 22:43:00 incoming pub count = 1000000. time taken for incoming pubs = 3m42.242271385s
```

**rumqttd**

```
2017/07/23 22:35:12 time taken for publishes = 3m35.521259143s
load average 1 =  2.931640625
average system memory =  2318325979
2017/07/23 22:35:13 incoming pub count = 1000000. time taken for incoming pubs = 3m36.247010478s
```

**emqttd (2.1.2)**

```
2017/07/23 22:50:09 time taken for publishes = 5m21.976169243s
load average 1 =  3.16748046875
average system memory =  2469802635
2017/07/23 22:50:15 incoming pub count = 999998. time taken for incoming pubs = 5m27.802035293s
```


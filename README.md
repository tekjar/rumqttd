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
2017/07/23 06:53:08 time taken for publishes = 3m43.144186301s
load average 1 =  2.57666015625
average system memory =  2392426064
2017/07/23 06:53:09 incoming pub count = 1000000. time taken for incoming pubs = 3m43.894361458s
```

**rumqttd**

```
2017/07/23 07:04:15 time taken for publishes = 3m34.021949483s
load average 1 =  2.66455078125
average system memory =  2417896266
2017/07/23 07:04:16 incoming pub count = 1000000. time taken for incoming pubs = 3m34.770672216s
```

**emqttd (2.1.2)**

```
2017/07/23 07:13:29 time taken for publishes = 5m22.936786039s
load average 1 =  2.70751953125
average system memory =  2499483235
2017/07/23 07:13:35 missing publishes. incoming pub count = 999999. time taken for incoming pubs = 5m28.707079321s
```


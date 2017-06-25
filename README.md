# rumqttd
high performance tokio based rust mqtt broker

# features

* MQTT 3.1.1
* QoS 0, 1, 2 Publishes & Subscribes



# todo

- [X] config file
- [X] topic wild cards
- [X] distinguish clean session & persistent session
- [X] last will
- [X] retained message
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
2017/05/13 18:26:25 time taken for publishes = 3m23.057874425s
load average 1 =  3.14501953125
average system memory =  2561352043
2017/05/13 18:26:25 incoming pub count = 1000000. time taken for incoming pubs = 3m23.943611846s
```

**rumqttd**

```
2017/05/13 18:22:08 time taken for publishes = 3m21.243963316s
load average 1 =  2.671875
average system memory =  2606343264
2017/05/13 18:22:09 incoming pub count = 1000000. time taken for incoming pubs = 3m22.055929751s
```

**emqttd (2.1.2)**

```
2017/05/13 18:49:00 time taken for publishes = 5m41.25939293s
load average 1 =  2.982421875
average system memory =  2519452728
2017/05/13 18:49:06 missing publishes. incoming pub count = 999998. time taken for incoming pubs = 5m47.267925698s
```


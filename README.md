# rumqttd
high performance tokio based rust mqtt broker

# features

* MQTT 3.1.1
* QoS 0, 1, 2 Publishes & Subscribes



### todo

- [ ] config file
- [ ] ignore invalid publishes with wildcard
- [ ] topic wild cards
- [ ] distinguish clean session & persistent session
- [ ] last will
- [ ] retained message
- [ ] username password authentication
- [ ] tls
- [ ] shared subscription
- [ ] graphana based dashboard plugin
- [ ] clustering

### benchmarks

Sending 5lakh qos1 publishes (and wait for acks) to broker on local machine

processor: 1.4 GHz Intel Core i5
memory: 4 GB 1600 MHz DDR3

**mosquitto (1.4.11)**

2017/05/09 05:03:57 time taken = 1m16.076848379s
load average 1 =  2.25341796875
average system memory =  2472544741

**rumqttd**

2017/05/09 04:56:23 time taken = 1m19.853062634s
load average 1 =  1.962890625
average system memory =  2500233241

**emqttd (2.1.2)**

2017/05/09 05:08:20 time taken = 2m15.176114959s
load average 1 =  2.177734375
average system memory =  2452130929



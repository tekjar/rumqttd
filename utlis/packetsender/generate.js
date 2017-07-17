#!/usr/local/bin/node

var mqtt = require('mqtt-packet')
var net = require('net')

var connect = {
  cmd: 'connect',
  protocolId: 'MQTT', // Or 'MQIsdp' in MQTT 3.1.1
  protocolVersion: 4, // Or 3 in MQTT 3.1
  clean: true, // Can also be false
  clientId: 'hello',
  keepalive: 10, // Seconds which can be any positive number, with 0 as the default setting
}

// console.log('connect: ',  mqtt.generate(connect))

var connack = {
  cmd: 'connack',
  returnCode: 0, // Or whatever else you see fit
  sessionPresent: true // Can also be true.
}

// console.log('connack: ',  mqtt.generate(connack))

var publish = {
  cmd: 'publish',
  messageId: 100,
  qos: 1,
  dup: false,
  topic: 'hello',
  payload: new Buffer('z'.repeat(120)),
  retain: false
}

// console.log('publish: ',  mqtt.generate(publish))

var puback = {
  cmd: 'puback',
  messageId: 1
}
// console.log('puback: ',  mqtt.generate(puback))

var pubrec = {
  cmd: 'pubrec',
  messageId: 1
}

var pubrel = {
  cmd: 'pubrel',
  messageId: 1
}

var pubcomp = {
  cmd: 'pubcomp',
  messageId: 1
}

var subscribe = {
  cmd: 'subscribe',
  messageId: 100,
  subscriptions: [
    {
      topic: 'hello/world',
      qos: 1
    },
    {
      topic: 'hello/crystal',
      qos: 2
    }
  ]
}

// console.log('subscribe: ',  mqtt.generate(subscribe))

var suback = {
  cmd: 'suback',
  messageId: 100,
  granted: [1, 2, 3, 128]
}

var disconnect = {
  cmd: 'disconnect'
}

// console.log('publish: ',  mqtt.generate(publish))

var client = new net.Socket();

client.connect(1883, '127.0.0.1', function () {

  process.argv.slice(2).forEach(function (val, index, array) {
    switch (val) {
      case 'connack':
        client.write(mqtt.generate(connack));
        break;
      case 'connect':
        client.write(mqtt.generate(connect));
        break;
      case 'publish':
        client.write(mqtt.generate(publish));
        break;
      case 'puback':
        client.write(mqtt.generate(puback));
        break;
      case 'pubrec':
        client.write(mqtt.generate(pubrec));
        break;
      case 'pubrel':
        client.write(mqtt.generate(pubrel));
        break;
      case 'pubcomp':
        client.write(mqtt.generate(pubcomp));
        break;
      case 'subscribe':
        client.write(mqtt.generate(subscribe));
        break;
      case 'suback':
        client.write(mqtt.generate(suback));
        break;
      case 'disconnect':
        client.write(mqtt.generate(disconnect));
        break;
    }
  });
});

setTimeout(function () {
  console.log('hello world!');
  client.end();
}, 5000);


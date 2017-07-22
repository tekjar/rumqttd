package main

import (
	"math/rand"
	"time"

	"strconv"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var counter uint64
var start = time.Now()
var end = time.Now()

const totalPublishes = 1000

var letters = []rune("z")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	opts := MQTT.NewClientOptions().AddBroker("tcp://localhost:1883")

	for i := 0; i < 1000; i++ {
		opts.SetClientID("mqtt-connection-" + strconv.Itoa(i))

		//create and start a client using the above ClientOptions
		c := MQTT.NewClient(opts)

		if token := c.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}

		time.Sleep(1 * time.Second)
	}

	time.Sleep(5 * time.Second)
}

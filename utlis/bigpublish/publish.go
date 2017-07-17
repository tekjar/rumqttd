package main

import (
	"log"
	"math/rand"
	"time"

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
	opts.SetClientID("mqtt-bigpublish")

	// opts.SetDefaultPublishHandler(msgHandler)

	//create and start a client using the above ClientOptions
	c := MQTT.NewClient(opts)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	for i := 0; i < totalPublishes; i++ {
		text := randSeq(1024)
		token := c.Publish("hello", 1, false, text)
		token.Wait()
	}

	log.Printf("time taken for publishes = %s", time.Since(start))
	time.Sleep(1 * time.Second)
}

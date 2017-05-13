package main

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	STATS "github.com/akhenakh/statgo"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func stats(exit chan bool) {
	ticker := time.NewTicker(time.Second * 1).C
	stats := STATS.NewStat()

	memory := []int{}

	for {
		select {
		case <-ticker:
			mem := stats.MemStats()
			memory = append(memory, mem.Used)
		case <-exit:
			cpu := stats.CPUStats()
			var totalMem int
			for _, value := range memory {
				totalMem += value
			}
			fmt.Println("load average 1 = ", cpu.LoadMin1)
			fmt.Println("average system memory = ", totalMem/len(memory))
			return
		}
	}
}

var counter uint64
var start = time.Now()
var end = time.Now()

const totalPublishes = 1000000

func main() {
	exit := make(chan bool, totalPublishes)
	statExit := make(chan bool)

	go stats(statExit)

	opts := MQTT.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetClientID("mqtt-benchmark")

	msgHandler := func(client MQTT.Client, msg MQTT.Message) {
		c := atomic.AddUint64(&counter, 1)
		exit <- false

		if c >= totalPublishes {
			exit <- true
			statExit <- true
		}
	}

	// opts.SetDefaultPublishHandler(msgHandler)

	//create and start a client using the above ClientOptions
	c := MQTT.NewClient(opts)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := c.Subscribe("hello/+/rumqtt", 1, msgHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	for i := 0; i < totalPublishes; i++ {
		text := fmt.Sprintf("this is msg #%d!", i)
		token := c.Publish("hello/mqtt/rumqtt", 1, false, text)
		token.Wait()
	}

	log.Printf("time taken for publishes = %s", time.Since(start))

L:
	for {
		select {
		case e := <-exit:
			if e {
				break L
			}
		case <-time.After(5 * time.Second):
			log.Printf("missing publishes. incoming pub count = %d. time taken for incoming pubs = %s", atomic.LoadUint64(&counter), time.Since(start))
			break L
		}
	}

	log.Printf("incoming pub count = %d. time taken for incoming pubs = %s", atomic.LoadUint64(&counter), time.Since(start))
	time.Sleep(1 * time.Second)
}

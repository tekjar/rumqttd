package main

import (
	"fmt"
	"log"
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

func main() {
	exit := make(chan bool)
	go stats(exit)

	start := time.Now()
	opts := MQTT.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetClientID("mqtt-benchmark")

	//create and start a client using the above ClientOptions
	c := MQTT.NewClient(opts)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	for i := 0; i < 500000; i++ {
		// fmt.Println(i)
		text := fmt.Sprintf("this is msg #%d!", i)
		token := c.Publish("go-mqtt/sample", 1, false, text)
		token.Wait()
	}

	elapsed := time.Since(start)
	log.Printf("time taken = %s", elapsed)
	exit <- true
	time.Sleep(10 * time.Second)
}

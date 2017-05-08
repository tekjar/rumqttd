package main

import MQTT "github.com/eclipse/paho.mqtt.golang"
import "fmt"
import "time"

func main() {
	opts := MQTT.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetClientID("go-simple")

	//create and start a client using the above ClientOptions
	c := MQTT.NewClient(opts)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	for i := 0; i < 500; i++ {
		fmt.Println(i)
		text := fmt.Sprintf("this is msg #%d!", i)
		token := c.Publish("go-mqtt/sample", 1, false, text)
		token.Wait()
	}

	time.Sleep(5 * time.Second)
}

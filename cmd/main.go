package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/wawancallahan/go-nsq/internal"
)

func main() {
	nsqMessage, err := internal.NewNsqConfig()

	if err != nil {
		log.Fatal("NSQ Not Running Well")
	}

	//Init topic name and message
	topic := "Topic_Example"
	msg := internal.Message{
		Name:      "Message Name Example",
		Content:   "Message Content Example",
		Timestamp: time.Now().String(),
	}
	//Convert message as []byte
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Println(err)
	}

	//Publish the Message
	err = nsqMessage.Producer.Publish(topic, payload)
	if err != nil {
		log.Println(err)
	}

	log.Println("Success")
}

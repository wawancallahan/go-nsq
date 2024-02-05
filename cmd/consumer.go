package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/wawancallahan/go-nsq/internal"
)

func main() {
	nsqMessage, err := internal.NewNsqConfig()

	if err != nil {
		log.Fatal("NSQ Not Running Well")
	}

	consumer_channel, _ := nsqMessage.NewConsumer("Channel_Example")
	consumer_channel_dev, _ := nsqMessage.NewConsumer("Channel_DEV_Example")

	defer func() {
		log.Println("Gracefully Stopping Consumer")

		consumer_channel.Stop()
		consumer_channel_dev.Stop()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Gracefully Shutdown")
}

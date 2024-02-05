package internal

import (
	"encoding/json"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
)

type Message struct {
	Name      string
	Content   string
	Timestamp string
}

type MessageHandler struct{}

// HandleMessage implements the Handler interface.
func (h *MessageHandler) HandleMessage(m *nsq.Message) error {
	//Process the Message
	var request Message
	if err := json.Unmarshal(m.Body, &request); err != nil {
		log.Println("Error when Unmarshaling the message body, Err : ", err)
		// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
		return err
	}
	//Print the Message
	log.Println("Message")
	log.Println("--------------------")
	log.Println("Name : ", request.Name)
	log.Println("Content : ", request.Content)
	log.Println("Timestamp : ", request.Timestamp)
	log.Println("--------------------")
	log.Println("")
	// Will automatically set the message as finish
	return nil
}

type NsqConfig struct {
	Config   *nsq.Config
	Producer *nsq.Producer
}

func NewNsqConfig() (*NsqConfig, error) {
	//The only valid way to instantiate the Config
	config := nsq.NewConfig()

	//Creating the Producer using NSQD Address
	producer, err := nsq.NewProducer("127.0.0.1:4150", config)
	if err != nil {
		log.Fatal("Error", err)
	}

	return &NsqConfig{
		Config:   config,
		Producer: producer,
	}, nil
}

func (n *NsqConfig) NewConsumer(channel string) (*nsq.Consumer, error) {
	//Tweak several common setup in config
	// Maximum number of times this consumer will attempt to process a message before giving up
	n.Config.MaxAttempts = 10
	// Maximum number of messages to allow in flight
	n.Config.MaxInFlight = 5
	// Maximum duration when REQueueing
	n.Config.MaxRequeueDelay = time.Second * 900
	n.Config.DefaultRequeueDelay = time.Second * 0

	//Init topic name and channel
	topic := "Topic_Example"

	//Creating the consumer
	consumer, err := nsq.NewConsumer(topic, channel, n.Config)
	if err != nil {
		log.Fatal("Error", err)
	}

	consumer.AddHandler(&MessageHandler{})

	consumer.ConnectToNSQLookupd("127.0.0.1:4161")

	return consumer, nil
}

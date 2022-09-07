package main

import (
	"log"

	"github.com/Shopify/sarama"

	cfg "sarama-learning/config"
)

func main() {
	c, err := cfg.Load()
	if err != nil {
		log.Panic(err)
	}
	// setup
	brokerList := []string{c.KafkaBrokers}
	topic := c.KafkaTopic
	maxRetry := 5

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Panic(err.Error())
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Something Cool 2"),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}

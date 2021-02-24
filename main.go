package main

import (
	"bufio"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
)

type Producer struct {
	ChatProducer sarama.SyncProducer
}

func (p *Producer) Close() error {
	err := p.ChatProducer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (p *Producer) SendStringData(message string) error {
	partition, offset, err := p.ChatProducer.SendMessage(&sarama.ProducerMessage{
		Key: sarama.StringEncoder("swing"),
		Topic: "message",
		Value: sarama.StringEncoder(message),
	})
	if err != nil {
		return err
	}
	fmt.Printf("%d/%d\n", partition, offset)
	return nil
}

func NewProducer() *Producer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	c, err := sarama.NewSyncProducer([]string{
		"localhost:9092",
		"localhost:9093",
		"localhost:9094"}, config)
	if err != nil {
		panic(err)
	}
	return &Producer{ChatProducer: c}
}

func main() {
	p := NewProducer()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		message, _ := reader.ReadString('\n')
		if message == "quit\n" {
			break
		}
		p.SendStringData(message)
	}
}
package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"time"
)

func produce(kConfig *kafka.ConfigMap, topic string, start, end int) error {
	p, err := kafka.NewProducer(kConfig)
	if err != nil {
		return fmt.Errorf("failed to create producer: %s", err)
	}
	defer p.Close()
	fmt.Printf("created producer %v\n", p)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				fmt.Printf("error: %v\n", ev)
			default:
				fmt.Printf("ignored event: %s\n", ev)
			}
		}
	}()
	c := start
	for c <= end {
		value := fmt.Sprintf("producer example, message #%d", c)
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}, nil)
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				time.Sleep(time.Second)
				continue
			}
			fmt.Printf("failed to produce message: %v\n", err)
		}
		c++
	}
	for p.Flush(10000) > 0 {
		fmt.Print("still waiting to flush outstanding messages\n")
	}
	return nil
}

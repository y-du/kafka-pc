package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"os/signal"
	"syscall"
)

func consume(kConfig *kafka.ConfigMap, topic, consumerGrp string) error {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	kConfig.SetKey("broker.address.family", "v4")
	kConfig.SetKey("group.id", consumerGrp)
	kConfig.SetKey("session.timeout.ms", 6000)
	kConfig.SetKey("auto.offset.reset", "earliest")
	kConfig.SetKey("enable.auto.offset.store", false)
	c, err := kafka.NewConsumer(kConfig)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %s", err)
	}
	fmt.Printf("created consumer %v\n", c)
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topics: %s\n", err)
	}
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% headers: %v\n", e.Headers)
				}
				_, err := c.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% error storing offset after message %s:\n",
						e.TopicPartition)
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
	fmt.Printf("closing consumer\n")
	return nil
}

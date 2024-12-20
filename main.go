package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"os"
)

type config struct {
	Host        string `json:"host"`
	Topic       string `json:"topic"`
	MsgStartNum int    `json:"msg_start_num"`
	MsgEndNum   int    `json:"msg_end_num"`
	ConsumerGrp string `json:"consumer_grp"`
	Action      string `json:"action"`
}

var defaultConfig = config{
	Host:        "kafka.kafka:9092",
	MsgStartNum: 1,
	MsgEndNum:   100,
}

func main() {
	file, err := os.Open("conf.json")
	if err != nil {
		if os.IsNotExist(err) {
			uid, err := uuid.NewUUID()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			defaultConfig.Topic = "test-topic-" + uid.String()
			defaultConfig.ConsumerGrp = "test-consumer-" + uid.String()
			if file, err = os.Create("conf.json"); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			defer file.Close()
			encoder := json.NewEncoder(file)
			encoder.SetIndent("", "  ")
			if err = encoder.Encode(defaultConfig); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			return
		}
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var conf config
	err = decoder.Decode(&conf)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	kConf := &kafka.ConfigMap{"bootstrap.servers": conf.Host}
	switch conf.Action {
	case "c":
		if err = consume(kConf, conf.Topic, conf.ConsumerGrp); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "p":
		if err = produce(kConf, conf.Topic, conf.MsgStartNum, conf.MsgEndNum); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	default:
		fmt.Println("invalid action")
		os.Exit(1)
	}
}

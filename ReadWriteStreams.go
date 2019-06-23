package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	runtime.GOMAXPROCS(20)

	inputMessageChan := make(chan []byte)
	doneChan := make(chan bool)
	go readDataFromFile(inputMessageChan)
	go produceKafkaMessages(inputMessageChan, doneChan)
	<-doneChan

	messageChan := make(chan []byte)
	writeDoneChan := make(chan bool)
	go consumeKafkaMessages(messageChan)
	go writeKafkaMessageToFile(messageChan, writeDoneChan)
	<-writeDoneChan
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func readDataFromFile(inputMessageChan chan []byte) {
	file, err := os.OpenFile("inputFile.txt", os.O_RDONLY, 0777)
	checkErr(err)

	reader := bufio.NewReader(file)
		for {
			line, _, err := reader.ReadLine()
			if err == io.EOF {
				break
			}
			inputMessageChan <- line
		}

	defer file.Close()
	close(inputMessageChan)
}

func produceKafkaMessages(inputMessageChan chan []byte, doneChan chan bool) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})

	checkErr(err)

	go func() {
		for e := range producer.Events() {
			switch event := e.(type) {
			case *kafka.Message:
				if event.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", event.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", event.TopicPartition)
				}
			}
		}
	}()

	topic := "my-replicated-topic"
	for data := range inputMessageChan {
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          data,
		}, nil)
	}

	producer.Flush(15 * 1000)
	producer.Close()
	doneChan <- true
}

func consumeKafkaMessages(messageChan chan []byte) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "test-consumer-group",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
	})

	checkErr(err)

	//defer c.Close()

	c.SubscribeTopics([]string{"my-replicated-topic"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			messageChan <- msg.Value
		} else {
			c.Close()
			checkErr(err)
		}
	}
}

func writeKafkaMessageToFile(messageChan chan []byte, writeDoneChan chan bool) {

	file, err := os.OpenFile("OutputFile.txt", os.O_APPEND|os.O_WRONLY, 0777)

	checkErr(err)

	for message := range messageChan {
		file.WriteString(time.Now().Format(time.Stamp) + "\n")
		_, err := file.Write(message)
		checkErr(err)
		file.WriteString("\n\n")
		checkErr(err)
		//fmt.Printf("Wrote message %v\n", string(message))
	}

	file.Close()
	writeDoneChan <- true
}

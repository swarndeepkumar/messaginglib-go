 package messaging
import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
	"sync"
	"errors"
)
var wg = sync.WaitGroup{}
func Consumemessage(config map[string]string, topicsarr []string, callback func(callbkerr error, callbkdata []string)) bool {
        if len(config) == 0 || len(topicsarr) == 0 {
		fmt.Println("consumemessage: config or topic or callback is invalid");
		callback(errors.New("config or topic is invalid"),[]string{"invalid input"});
		return false
	}
	wg.Add(1)
	// called consumer goroutine
	go consumerRoutine(config,topicsarr, callback)
	return true
}

func consumerRoutine(config map[string]string, topicsarr []string, callback func(callbkerr error, callbkdata []string)){
	defer wg.Done()
	message := []string{}
	consConfig :=config
	var configs = kafka.ConfigMap{}
        for key, value := range consConfig {
                configs[key] = value 
		if key == "default.topic.config" {
			configs["default.topic.config"] = kafka.ConfigMap{"auto.offset.reset": "earliest"}
		} 
        }
	//configs["default.topic.config"] = kafka.ConfigMap{"auto.offset.reset": "earliest"}
	c, err := kafka.NewConsumer(&configs)
        if err != nil {
                fmt.Printf("Consumemessage: Failed to create consumer: %s\n", err)
                callback(err,message);
        }else {

        fmt.Printf("Created Consumer %v\n", c)
        err = c.SubscribeTopics(topicsarr, nil)
        if err != nil {
                callback(err, message);
        }
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	messageArray := []string{}
	 run := true
        for run == true {
                select {
                case sig := <-sigchan:
                        fmt.Printf("Caught signal %v: terminating\n", sig)
                        run = false
                default:
                        ev := c.Poll(100)
                        if ev == nil {
                                continue
                        }

                        switch e := ev.(type) {
                        case *kafka.Message:
                                //fmt.Printf("%% Message on %s:\n%s\n",
                                  //      e.TopicPartition, string(e.Value))
				messageArray = append(messageArray,string(e.Value))
                        case kafka.PartitionEOF:
                                //fmt.Printf("%% Reached %v\n", e)
				callback(nil, messageArray)
                        case kafka.Error:
                                fmt.Fprintf(os.Stderr, "%% Error: %v\n", e) 
				callback(e, messageArray)
                                run = false
                        default:
                               // fmt.Printf("Ignored %v\n", e)
                        }
                }
        }
  }
        fmt.Printf("Closing consumer\n")
        c.Close()
}


func Producemessage(config map[string]string , topics string , values string, callback func(callbkerr error, callbkdata string)) bool {
	if len(config) == 0 || len(topics) == 0 || len(values) == 0 {
                fmt.Println("Producemessage: config or topics or message or callback is invalid");
		callback(errors.New("config or topic or message is invalid"), "invalid input");
                return false
        }
	wg.Add(1)
	go producerRoutine(config, topics, values, callback)
	return true
}

func producerRoutine(config map[string]string, topic string, message string, callback func(callbkerr error, callbkdata string)){
	defer wg.Done()
        var configs = kafka.ConfigMap{}
        for key, value := range config {
                configs[key] = value
        }

	 p, err := kafka.NewProducer(&configs)

        if err != nil {
                fmt.Printf("Failed to create producer: %s\n", err)
                callback(err, "failed")
        } else {

        fmt.Printf("Created Producer %v\n", p)

	 deliveryChan := make(chan kafka.Event)

        value := message
        err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)}, deliveryChan)

        e := <-deliveryChan
        m := e.(*kafka.Message)

        if m.TopicPartition.Error != nil {
                fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
                callback(m.TopicPartition.Error, "Delivery failed");
        } else {
                fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
                        *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	    callback(nil,"success")
        }
        close(deliveryChan)
    }

}


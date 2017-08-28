
package producer


import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func Producemessage(brokers string , topics string , values string, callback func(callbkerr error, callbkdata string)) {
        /*
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic>\n",
			os.Args[0])
		os.Exit(1)
	}
        */
	broker := brokers
	topic := topics

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		callback(err, "failed");
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)
       /// callback 
	//var callbkerr string = "myerrorec"
        //var callbkdata string = "mydbrec"
        //callback(callbkerr,callbkdata);
	/// callback end 


	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	value := values
	err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		callback(m.TopicPartition.Error, "Delivery failed");
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
        callback(nil, "success");
	close(deliveryChan)
}

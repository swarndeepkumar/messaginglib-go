package messaging
import (
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
)

func TestConsumerMethodsWithoutTopic(t *testing.T){
var consConfig = map[string]string{"bootstrap.servers":"localhost:9092","group.id":"testgroup","session.timeout.ms":"6000"}
var topics = []string {}
	// callback method, that will be called after completing the method
        var callbackconsumer = func(err error , data []string){
		 if err != nil {
                        // write your success code, bec message is produced
                       // fmt.Println("consumeTopic: error :", err);
			t.Logf("it returning error message")
                }else{
                        // write your falure code, bec message could not produced
                       // fmt.Println("consumeTopic demo 1 application: success: recieved message:",data);
			 assert.Equal(t, true, false, "It should return error")
                }

        }
 result := Consumemessage(consConfig, topics, callbackconsumer)
 assert.Equal(t, result ,false,"it should retun false");
}

func TestConsumerMethodsWithoutConfig(t *testing.T){

var consConfig = map[string]string{}
var topics = []string {"topic1"}
        // callback method, that will be called after completing the method
        var callbackconsumer = func(err error , data []string){
                 if err != nil {
                        // write your success code, bec message is produced
                       // fmt.Println("consumeTopic: error :", err);
                        t.Logf("it returning error message")
                }else{
                        // write your falure code, bec message could not produced
                       // fmt.Println("consumeTopic demo 1 application: success: recieved message:",data);
                         assert.Equal(t, true, false, "It should return error")
                }

        }
 result := Consumemessage(consConfig, topics, callbackconsumer)
 assert.Equal(t, result ,false,"it should retun false");

}
func TestProducerMethodsWithoutTopic(t *testing.T){
// assert.Equal(t, 123, 123, "they should be equal")
        pmessage := "testmessage"
	tname :=  ""
	// prodcuer configuration
	var prodConfig = map[string]string{"bootstrap.servers":"localhost:9092"}
	// callback method, that will be called after completing the method 
        var callback = func(err error, data string){
		if err != nil { 
			// write your success code, bec message is produced
			t.Logf("it returning error message")
		}else{ 
 			// write your falure code, bec message could not produced
			assert.Equal(t, true, false, "it should return error") 
		}
          }
 result := Producemessage(prodConfig, tname, pmessage, callback)
 assert.Equal(t, result ,false,"it should retun false");
}
func TestProducerMethodsWithoutMessage(t *testing.T){
// assert.Equal(t, 123, 123, "they should be equal")
        pmessage := ""
        tname :=  "testtopic"
        // prodcuer configuration
        var prodConfig = map[string]string{"bootstrap.servers":"localhost:9092"}
        // callback method, that will be called after completing the method
        var callback = func(err error, data string){
                if err != nil {
                        // write your success code, bec message is produced
                        t.Logf("it returning error message")
                }else{
                        // write your falure code, bec message could not produced
                        assert.Equal(t, true, false, "it should return error")
                }
          }
 result := Producemessage(prodConfig, tname, pmessage, callback)
 assert.Equal(t, result ,false,"it should retun false");
}

func TestProducerMethodsWithoutConfig(t *testing.T){
// assert.Equal(t, 123, 123, "they should be equal")
        pmessage := "testmessage"
        tname :=  "testtopic"
        // prodcuer configuration
        var prodConfig = map[string]string{}
        // callback method, that will be called after completing the method
        var callback = func(err error, data string){
                if err != nil {
                        // write your success code, bec message is produced
                        t.Logf("it returning error message")
                }else{
                        // write your falure code, bec message could not produced
                        assert.Equal(t, true, false, "it should return error")
                }
          }
 result := Producemessage(prodConfig, tname, pmessage, callback)
 assert.Equal(t, result ,false,"it should retun false");
}

func TestConDown(t *testing.T){
	var consConfig = map[string]string{"bootstrap.servers":"localhost:xxxx","group.id":"testg","session.timeout.ms":"1000"}
	var topics = []string {"topiccode1","topiccode2"}
	var callbackconsumer = func(err error , data []string){
		 if err != nil {
                        // write your success code, bec message is produced
			 t.Logf("it returning error message")
                }else{
                        // write your falure code, bec message could not produced
			 t.Logf("it returning data")
                }

        }
	result := Consumemessage(consConfig, topics, callbackconsumer)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, result ,true,"it should retun true");

}
func TestProdDown(t *testing.T){
	var pmessage = "testing message"
	var tname = "topicname"
        var prodConfig = map[string]string{"bootstrap.servers":"localhost:xxxx"}
	// callback method, that will be called after completing the method 
        var callback = func(err error, data string){
		if err != nil { 
			// write your success code, bec message is produced
			 t.Logf("it returning error message")
		}else{ 
 			// write your falure code, bec message could not produced
			 t.Logf("it returning data")
		}
          }
        result := Producemessage(prodConfig, tname, pmessage, callback)
        time.Sleep(1200 * time.Millisecond)
        assert.Equal(t, result ,true,"it should retun true");

        //fmt.Println(c)
}


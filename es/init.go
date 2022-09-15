package es

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/olivere/elastic/v7"
	"log"
)

var client = new(elastic.Client)
var msgChan chan *sarama.ConsumerMessage

type ElasticObj struct {
	Time  string `json:"time"`
	Value string `json:"detail"`
}

func Init(url string) (err error) {
	sniff := elastic.SetSniff(false)
	client, err = elastic.NewClient(elastic.SetURL(url), sniff)
	msgChan = make(chan *sarama.ConsumerMessage)
	go SendToES()
	return err
}
func SendToChan(msg *sarama.ConsumerMessage) {
	msgChan <- msg
}

func SendToES() {
	for {
		msg := <-msgChan
		var body = ElasticObj{Time: msg.Timestamp.String(), Value: string(msg.Value)}
		_, err := client.Index().Index(msg.Topic).BodyJson(body).Do(context.Background())
		if err != nil {
			log.Println("sendToES error", err)
		}
		fmt.Println("send successful")
	}
}

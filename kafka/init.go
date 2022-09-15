package kafka

import (
	"consum/conf"
	"consum/es"
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

var consumer sarama.Consumer
var consumeMgr = new(ConsumeMgr)

type ConsumeMgr struct {
	ConsumeControl map[string]ContextAndCancel
}
type CollectEntry struct {
	Topic string `mapstructure:"topic" validate:"required"`
}
type ContextAndCancel struct {
	Ctx    context.Context
	Cancel context.CancelFunc
}

func Init(config *conf.KafkaConfig) (err error) {
	consumer, err = sarama.NewConsumer(config.Address, nil)
	return err
}
func (c *ConsumeMgr) ConsumeALL() {
	for k := range c.ConsumeControl {
		go Consume(c.ConsumeControl[k].Ctx, k)
	}
}
func ConsumeMgrInit(confList []*CollectEntry) {
	consumeMgr.ConsumeControl = make(map[string]ContextAndCancel)
	for _, v := range confList {
		ctx, cancel := context.WithCancel(context.Background())
		consumeMgr.ConsumeControl[v.Topic] = ContextAndCancel{Ctx: ctx, Cancel: cancel}
	}
}
func GetConsumeMgr() *ConsumeMgr {
	return consumeMgr
}
func Consume(ctx context.Context, topic string) {

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("success to consume", topic)
	for partition := range partitions { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {

			for {
				select {
				case <-ctx.Done():
					fmt.Printf("topic %s done\n", topic)
					_ = pc.Close()
					return
				case msg := <-pc.Messages():
					fmt.Printf("sending to Chan....... ")
					es.SendToChan(msg)
				}
			}
		}(pc)
	}

	return
}

package etcd

import (
	"consum/conf"
	"consum/kafka"
	"context"
	"encoding/json"
	"errors"
	"go.etcd.io/etcd/client/v3"
	"time"
)

var client = new(clientv3.Client)

func Init() (err error) {
	client, err = clientv3.New(clientv3.Config{Endpoints: conf.GetConf().Etcd.Address, DialTimeout: time.Second * 5})
	go watch()
	return err
}
func GetConfList() (confList []*kafka.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	get, err := client.Get(ctx, conf.GetConf().Etcd.CollectKey)
	if err != nil {
		return
	}
	if len(get.Kvs) == 0 {
		return nil, errors.New("etcd get len:0")
	}
	err = json.Unmarshal(get.Kvs[0].Value, &confList)
	return
}
func watch() {

	watchChan := client.Watch(context.Background(), conf.GetConf().Etcd.CollectKey)
	for {

		select {
		case <-watchChan:
			list, err := GetConfList()
			if err != nil {
				continue
			}
			var hash = map[string]bool{}
			consumeMgr := kafka.GetConsumeMgr()
			for _, v := range list {
				hash[v.Topic] = true
				if _, ok := consumeMgr.ConsumeControl[v.Topic]; !ok {
					ctx, cancel := context.WithCancel(context.Background())
					consumeMgr.ConsumeControl[v.Topic] = kafka.ContextAndCancel{Ctx: ctx, Cancel: cancel}
					go kafka.Consume(ctx, v.Topic)
				}
			}
			for k, v := range consumeMgr.ConsumeControl {
				if !hash[k] {
					delete(consumeMgr.ConsumeControl, k)
					v.Cancel()
				}
			}

		}
	}
}

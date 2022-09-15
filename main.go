package main

import (
	"consum/conf"
	"consum/es"
	"consum/etcd"
	"consum/kafka"
	_ "net/http/pprof"
)

func init() {
	err := conf.Init()
	if err != nil {
		panic(err)
	}
	err = es.Init(conf.GetConf().Es.Url)
	if err != nil {
		panic(err)
	}
	err = kafka.Init(conf.GetConf().Kafka)
	if err != nil {
		panic(err)
	}
	err = etcd.Init()
	if err != nil {
		panic(err)
	}
}
func main() {
	list, err := etcd.GetConfList()
	if err != nil {
		panic(err)
	}
	kafka.ConsumeMgrInit(list)
	kafka.GetConsumeMgr().ConsumeALL()
	select {}
}

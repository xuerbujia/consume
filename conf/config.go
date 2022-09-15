package conf

import "github.com/spf13/viper"

type EsConfig struct {
	Url string `mapstructure:"url"`
}
type KafkaConfig struct {
	Address []string `mapstructure:"address"`
}
type Config struct {
	Es    *EsConfig    `mapstructure:"es"`
	Kafka *KafkaConfig `mapstructure:"kafka"`
	Etcd  *EtcdConfig  `mapstructure:"etcd"`
}
type EtcdConfig struct {
	Address    []string `mapstructure:"address"`
	CollectKey string   `mapstructure:"collect_key"`
}

var conf = new(Config)

func GetConf() *Config {
	return conf
}
func Init() (err error) {
	viper.SetConfigFile("./conf/config.yml")
	err = viper.ReadInConfig()
	if err != nil {
		return err
	}
	return viper.Unmarshal(conf)
}

package master

import (
	"encoding/json"
	"io/ioutil"
)

// 程序配置
type Config struct {
	ApiPort         int `json:"apiPort"`
	ApiReadTimeout  int `json:"apiReadTimeout"`
	ApiWriteTimeout int `json:"apiWriteTimeout"`
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	WebRoot string `json:"webroot"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
}

var G_config *Config


// 加载配置
func  InitConfig(filename string) (err error) {
	var conf Config
	// 1, 把配置文件读进来
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	// 2, 做JSON反序列化
	 err = json.Unmarshal(content, &conf)
	 if err != nil {
		return
	}

	// 3, 赋值单例
	G_config = &conf

	return
}

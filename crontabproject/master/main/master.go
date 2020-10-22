package main

import (
	"flag"
	"fmt"
	"learngo2/crontab/master"
	"runtime"
	"time"
)

var (
	confFile string // 配置文件路径
)

// 解析命令行参数
func initArgs() {
	// master -config ./master.json -xxx 123 -yyy ddd
	// master -h
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
	flag.Parse()
}

// 初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {


	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置
	err := master.InitConfig(confFile)
	if err != nil {
		goto ERR
	}
	err = master.InitWorkerMgr()
	if err != nil {
		goto ERR
	}
	err = master.InitLogMgr()
	if err != nil {
		goto ERR
	}
	//任务管理器
	err = master.InitJobMgr()
	if err != nil {
		goto ERR
	}

	// 启动Api HTTP服务
	 err = master.InitApiServer()
	 if err != nil {
		goto ERR
	}

	// 正常退出
for {
	time.Sleep(1 * time.Second)
}

	return

ERR:
	fmt.Println(err)
}

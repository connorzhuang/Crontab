package master

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"learngo2/crontab/common"
	"time"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var G_workerMgr *WorkerMgr


func (w *WorkerMgr) GetWorkerList()(workerIpArrs []string,err error){
	getResponse, err := w.kv.Get(context.TODO(), common.IP_Save_Dir, clientv3.WithPrefix())
	if err != nil {
		return
	}
	for _,kv := range getResponse.Kvs{
		workerIp := common.ExtractWorkerIP(string(kv.Key))
		workerIpArrs = append(workerIpArrs,workerIp)
	}
	return
}


func InitWorkerMgr()(err error){
	config := clientv3.Config{
		Endpoints: G_config.EtcdEndpoints,
		DialTimeout:5*time.Second,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return
	}

	kv:=clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	G_workerMgr = &WorkerMgr{
		client: client,
		kv: kv,
		lease: lease,
	}
	return
}
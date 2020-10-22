package worker

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"learngo2/crontab/common"
	"net"
	"time"
)

type Register struct {
	client *clientv3.Client
	kv   clientv3.KV
	lease clientv3.Lease

	localIP string
}

var G_register *Register

//获取本机网卡的IP
func getLocalIP()(localIP string,err error){
	var(
		ipNet *net.IPNet
		isIpNet bool
	)
	//获取所有网卡
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}

	for _,addr := range addrs{
		if ipNet,isIpNet = addr.(*net.IPNet);isIpNet&&!ipNet.IP.IsLoopback(){
			if ipNet.IP.To4() != nil {
				localIP = ipNet.IP.String()
				return
			}
		}
	}

	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}

func (r *Register) keepOnline(){
   var (
   	grant  *clientv3.LeaseGrantResponse
   	keepAliveChan  <-chan *clientv3.LeaseKeepAliveResponse
   	keepRes *clientv3.LeaseKeepAliveResponse
   	ctx context.Context
   	cancelFunc context.CancelFunc
   	err error
   )
for {
	key := common.IP_Save_Dir+r.localIP
	cancelFunc = nil
	grant, err = r.lease.Grant(context.TODO(), 10)
	if err != nil {
		goto RETRY
	}

	keepAliveChan, err = r.lease.KeepAlive(context.TODO(), grant.ID)
	if err != nil {
		goto RETRY
	}

	ctx, cancelFunc = context.WithCancel(context.TODO())
	if _, err = r.kv.Put(ctx, key, "", clientv3.WithLease(grant.ID));err != nil {
		goto RETRY
	}

	for {
		select {
		case keepRes = <-keepAliveChan:
			if keepRes == nil {
				goto RETRY
			}
		}
	}

	RETRY:
		time.Sleep(1*time.Second)
	if cancelFunc != nil {
		cancelFunc()
	}

}
}

func InitRegister()(err error){
	config := clientv3.Config{
		Endpoints: G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout)*time.Millisecond,
	}
	client, err := clientv3.New(config)
	if err != nil {
		panic(err)
	}
	kv := client.KV
	lease := client.Lease
	localIP, err := getLocalIP()
	if err != nil {
		return
	}
	G_register = &Register{
		client: client,
		kv: kv,
		lease: lease,
		localIP:localIP,
	}

	go G_register.keepOnline()
	return
}

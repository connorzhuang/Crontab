package master

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"learngo2/crontab/common"
	"time"
)
//任务管理器
type JobMgr struct {
	client * clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

}
//单例
var (
	G_jobMgr *JobMgr
)

func InitJobMgr() (err error){
	config := clientv3.Config{
		Endpoints: G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}
	client, err := clientv3.New(config)
	if err != nil {
		panic(err)
	}
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	G_jobMgr=&JobMgr{
		client: client,
		kv: kv,
		lease: lease,
	}
	return
}

func (j *JobMgr) JobSave(job *common.Job) (oldJob *common.Job,err error) {
	var oldJobObj common.Job
	jobKey := common.Job_Save_Dir+job.Name
	jobValue, err := json.Marshal(job)
	if err != nil {
		panic(err)
	}
	putResp, err := G_jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue),clientv3.WithPrevKV())
	if err != nil {
		panic(err)
	}
	//存在旧值
	if putResp.PrevKv != nil {
		//对旧值进行反序列化
		err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj)
		if err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

func (j *JobMgr) JobDelete(name string)(oldJob *common.Job,err error){
	var oldJobObj common.Job
	jobKey:=common.Job_Save_Dir+name
	deleteResp, err := G_jobMgr.kv.Delete(context.TODO(), jobKey,clientv3.WithPrevKV())
	if err != nil {
		return
	}
	if len(deleteResp.PrevKvs) != 0 {
		err = json.Unmarshal(deleteResp.PrevKvs[0].Value, &oldJobObj)
		if err != nil {
			return
		}
		oldJob = &oldJobObj
	}

	return
}

func (j *JobMgr) JobList() (jobList []*common.Job,err error){
	var job *common.Job
	DirKey := common.Job_Save_Dir
	getResp, err := G_jobMgr.kv.Get(context.TODO(), DirKey,clientv3.WithPrefix())
	if err !=nil {
		return
	}
	jobList = make([]*common.Job,0)

	for _,kvs := range getResp.Kvs{
		job = &common.Job{}
		err := json.Unmarshal(kvs.Value, job)
		if err != nil{
			continue
		}
		jobList = append(jobList,job)
	}

	return
}

func (j *JobMgr) JobKill(name string)(err error){
	KillKey := common.Job_Kill_Dir + name
	leaseResp, err := G_jobMgr.lease.Grant(context.TODO(), 1)
	if err != nil {
		return
	}
	leaseId := leaseResp.ID
	_, err = G_jobMgr.kv.Put(context.TODO(), KillKey, "", clientv3.WithLease(leaseId))
	if err != nil {
		return
	}

	return
}


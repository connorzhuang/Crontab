package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"learngo2/crontab/common"
	"time"
)
//任务管理器
type JobMgr struct {
	client * clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}
//单例
var (
	G_jobMgr *JobMgr
)
//监听任务变化
func (j *JobMgr) watchJobs() (err error){
	var jobEvent *common.JobEvent
	getResp, err := G_jobMgr.kv.Get(context.TODO(), common.Job_Save_Dir, clientv3.WithPrefix())
	if err != nil {
		return
	}
	for _,kvpair := range getResp.Kvs {
		job, err := common.UnpackJob(kvpair.Value)
		if err == nil {
			jobEvent := common.BuildJobEvent(common.Job_Save_Event, job)
			//把job同步到scheduler
			G_scheduler.PushJobEvent(jobEvent)
		}

	}
	go func() { //监听协程
		watcherRev := getResp.Header.Revision+1
		watchChan := G_jobMgr.watcher.Watch(context.TODO(), common.Job_Save_Dir, clientv3.WithRev(watcherRev),clientv3.WithPrefix())
		for watcherResp := range watchChan{
			for _,watchEvent := range watcherResp.Events{
				switch watchEvent.Type{
				case mvccpb.PUT://任务保存事件
					job, err := common.UnpackJob(watchEvent.Kv.Value)
					if err != nil {
						continue
					}
					//构建一个更新event
					jobEvent = common.BuildJobEvent(common.Job_Save_Event, job)
				//反序列化job，推一个更新事件给scheduler

				case mvccpb.DELETE://任务删除事件
				//获得任务名
					jobName := common.ExtractJobName(string(watchEvent.Kv.Key))

					job := &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.Job_Delete_Event, job)
					//推一个删除事件给scheduler

				}
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}
//创建任务执行锁
func (j *JobMgr) CreateJobLock(jobName string)(jobLock *JobLock){
	jobLock = InitJobLock(jobName, j.kv, j.lease)
	return
}

func (j *JobMgr) WatchKiller()(err error){

	go func() {
		watchChan := j.watcher.Watch(context.TODO(), common.Job_Kill_Dir, clientv3.WithPrefix())
		for {
			select {
			case watcherRes := <- watchChan:
				for _,event := range watcherRes.Events{
					switch event.Type {
					case mvccpb.PUT:
						jobName := common.ExtractKillerName(string(event.Kv.Key))
						job := &common.Job{Name: jobName}
						jobEvent := common.BuildJobEvent(common.Job_Kill_Event,job)
						G_scheduler.PushJobEvent(jobEvent)
					case mvccpb.DELETE:
					}
				}
			}
		}
	}()
	return
}

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
	watcher := clientv3.NewWatcher(client)
	G_jobMgr=&JobMgr{
		client: client,
		kv: kv,
		lease: lease,
		watcher: watcher,
	}
	//启动任务监听
		G_jobMgr.watchJobs()
	//启动监听killer
	    G_jobMgr.WatchKiller()
	return
}




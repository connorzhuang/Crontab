package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"learngo2/crontab/common"
)

type JobLock struct {
	jobName string

	kv clientv3.KV
	lease clientv3.Lease

	leaseId clientv3.LeaseID
	cancelFunc context.CancelFunc
	isLocked bool
}
func (j *JobLock) TryLock()(err error){
	resp, err := j.lease.Grant(context.TODO(), 5)
	if err != nil {
		return
	}
	leaseId := resp.ID
	ctx, cancelFunc := context.WithCancel(context.TODO())
	 keepAliveChan, err := j.lease.KeepAlive(ctx, leaseId)
	 if err != nil {
		return
	 }
	go func() {
		for  {
			select {
			case keepResp := <- keepAliveChan:
				if keepResp == nil {
					goto END
				}
			}
		}
		END:
	}()

	txn := j.kv.Txn(context.TODO())
	//锁路径
	 lockKey := common.Job_Lock_Dir+j.jobName

	 txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey),"=",0)).
	 	Then(clientv3.OpPut(lockKey,"",clientv3.WithLease(leaseId))).
	 	Else(clientv3.OpGet(lockKey))

	commit, err := txn.Commit()
	if err != nil {
		goto FAIL
	}
	if !commit.Succeeded {
		goto FAIL
	}

	//抢锁成功
	j.leaseId=leaseId
	j.cancelFunc=cancelFunc
	j.isLocked=true
	return

FAIL:
		cancelFunc()
	    j.lease.Revoke(context.TODO(),leaseId)

	return
}

func (j *JobLock) Unlock(){
	if j.isLocked {
		j.cancelFunc()
		j.lease.Revoke(context.TODO(), j.leaseId)
	}
}

func InitJobLock(jobName string,kv clientv3.KV,lease clientv3.Lease) (jobLock *JobLock){
	jobLock = &JobLock{
		jobName: jobName,
		kv: kv,
		lease: lease,
	}
	return
}

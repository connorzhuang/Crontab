package worker

import (
	"learngo2/crontab/common"
	"os/exec"
	"time"
)

type Executor struct {

}

var G_executor *Executor

func (e *Executor) ExecuteJob (info *common.JobExecuteInfo){
	go func() {
		result := &common.JobExecuteResult{
			JobExecuteInfo: info,
			Output:make([]byte,0),
		}

		//初始化分布式锁
		jobLock := G_jobMgr.CreateJobLock(info.Job.Name)

		result.StartTime=time.Now()
		err := jobLock.TryLock()
		defer jobLock.Unlock()
		if err != nil {
			result.Err =err
			result.EndTime=time.Now()
		}else {
			result.StartTime=time.Now()

			cmd := exec.CommandContext(info.CalCtx, "D:\\cygwin64\\bin\\bash.exe", "-c", info.Job.Command)
			output, err := cmd.CombinedOutput()
			if err != nil {
				return
			}
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}
		G_scheduler.PushJobExecuteResult(result)
	}()
}


//初始化任务执行器
func InitExecutor()(err error){
	G_executor=&Executor{}
	return
}
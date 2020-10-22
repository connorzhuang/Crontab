package worker

import (
	"fmt"
	"learngo2/crontab/common"
	"time"
)

type Scheduler struct {
	jobEventChan chan *common.JobEvent
	jobPlanTable map[string]*common.SchedulePlan
	jobExecutingTable map[string]*common.JobExecuteInfo
	jobResultChan chan *common.JobExecuteResult
}


var G_scheduler *Scheduler



//任务事件处理
func (s *Scheduler) handleJobEvent(jobEvent *common.JobEvent)  {
	var jobExisted bool
	var jobExcuting bool
	var jobExecuteInfo *common.JobExecuteInfo
	switch jobEvent.JobEventType {
	case common.Job_Save_Event://保存任务
		jobSchedulePlan, err := common.BuildJobSchedulePlan(jobEvent.Job)
		if err != nil {
			return
		}
		s.jobPlanTable[jobEvent.Job.Name]=jobSchedulePlan
	case common.Job_Delete_Event://删除任务
	 if jobExisted {
		delete(s.jobPlanTable,jobEvent.Job.Name)
	}
	case common.Job_Kill_Event:
		if jobExecuteInfo,jobExcuting = s.jobExecutingTable[jobEvent.Job.Name];jobExcuting{
			jobExecuteInfo.CancelFunc()
		}

	}

}
func (s *Scheduler)tryStartJob(plan *common.SchedulePlan){
	var jobExecuteInfo *common.JobExecuteInfo
	var jobExisting bool
	if jobExecuteInfo,jobExisting = s.jobExecutingTable[plan.Job.Name];jobExisting{
		fmt.Println("任务不予执行:",plan.Job.Name)
		return
	}
	jobExecuteInfo, err := common.BuildJobExecuteInfo(plan)
	if err != nil {
		return
	}
	s.jobExecutingTable[plan.Job.Name]=jobExecuteInfo

	//执行任务
	fmt.Println("执行任务：",jobExecuteInfo.Job.Name,jobExecuteInfo.PlanTime,jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)

}
//重新计算任务调度状态
func (s *Scheduler) trySchedule()(scheduleAfter time.Duration){
	var nearTime *time.Time
	now := time.Now()
	//如果任务列表为空
	if len(s.jobPlanTable)==0{
		scheduleAfter= 1*time.Second
		return
	}
	//遍历所有任务
	for _,jobPlan :=range s.jobPlanTable{
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now){
			s.tryStartJob(jobPlan)
			jobPlan.NextTime=jobPlan.Expr.Next(now) //更新下次任务调度时间
		}
		//统计最近一个要过期的任务时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime){
			nearTime =&jobPlan.NextTime
		}

	}
	//下次调度间隔
	scheduleAfter =(*nearTime).Sub(now)
	return
}
//执行结果处理
func (s *Scheduler) handleResult(jobResult *common.JobExecuteResult){
	var jobLog *common.JobLog
	delete(s.jobExecutingTable,jobResult.JobExecuteInfo.Job.Name)
	//生成执行日志
	if jobResult.Err != common.ERR_LOCK_ALREADY_REQUIRED{
		jobLog =&common.JobLog{
			JobName:jobResult.JobExecuteInfo.Job.Name,
			Command: jobResult.JobExecuteInfo.Job.Command,
			Output: string(jobResult.Output),
			PlanTime: jobResult.JobExecuteInfo.PlanTime.UnixNano()/1000/1000,
			ScheduleTime: jobResult.JobExecuteInfo.RealTime.UnixNano()/1000/1000,
			StartTime: jobResult.StartTime.UnixNano()/1000/1000,
			EndTime: jobResult.EndTime.UnixNano()/1000/1000,
		}
		if jobResult.Err != nil {
			jobLog.Err = jobResult.Err.Error()
		}else {
			jobLog.Err =""
		}
		G_jobSink.PushJobLog(jobLog)
	}

	fmt.Println("任务执行完成：",string(jobResult.Output))
}


func (s *Scheduler) schedulerLoop(){
	scheduleAfter := s.trySchedule()

	scheduleTimer :=time.NewTimer(scheduleAfter)
	for {
		select {
		case jobEvent:= <- s.jobEventChan:
			//对我们维护的任务进行增删改查
			s.handleJobEvent(jobEvent)
		case <- scheduleTimer.C:
		case jobResult := <- s.jobResultChan:
			 s.handleResult(jobResult)
			}
		scheduleAfter = s.trySchedule()
		//重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}
}


func ( s *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	s.jobEventChan <- jobEvent
}

func InitScheduler()(err error){
	G_scheduler=&Scheduler{
		jobEventChan: make(chan *common.JobEvent,100),
		jobPlanTable: make(map[string]*common.SchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobExecuteResult,100),
	}
	go G_scheduler.schedulerLoop()
	return
}

func (s *Scheduler) PushJobExecuteResult(result *common.JobExecuteResult){
	s.jobResultChan <- result
}
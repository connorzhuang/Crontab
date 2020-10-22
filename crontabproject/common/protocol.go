package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

type Job struct {

	Name string  `json:"name"`
	Command string   `json:"command"`
	CronExpr string   `json:"cronExpr"`
}

type JobLog struct {
	JobName string `json:"jobName" bson:"jobName"` // 任务名字
	Command string `json:"command" bson:"command"` // 脚本命令
	Err string `json:"err" bson:"err"` // 错误原因
	Output string `json:"output" bson:"output"`	// 脚本输出
	PlanTime int64 `json:"planTime" bson:"planTime"` // 计划开始时间
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"` // 实际调度时间
	StartTime int64 `json:"startTime" bson:"startTime"` // 任务执行开始时间
	EndTime int64 `json:"endTime" bson:"endTime"` // 任务执行结束时间
}

type LogMap struct {
	Logs []interface{}
}

//任务调度计划
type SchedulePlan struct {
	Job *Job
	Expr *cronexpr.Expression
	NextTime time.Time
}
//任务执行状态
type JobExecuteInfo struct {
	Job *Job
	PlanTime time.Time  //理论上调度的时间
	RealTime time.Time  //实际执行的时间
	CancelFunc context.CancelFunc
	CalCtx context.Context
}
//任务执行结果
type JobExecuteResult struct {
	JobExecuteInfo *JobExecuteInfo
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}

//查询日志的过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}
//任务日志排序规则
type SortJobLogByStartTime struct {
	SortOrder int `bson:"startTime"`
}


type Response struct {
	Errno int    `json:"errno"`
	Msg string    `json:"msg"`
	Data interface{}  `json:"data"`
}

type JobEvent struct {
	JobEventType int
	Job *Job
}


func BuildResponse(errno int,msg string,data interface{}) (resp []byte,err error){
	var response Response
	response.Errno= errno
	response.Msg=msg
	response.Data=data
	resp, err = json.Marshal(response)
	return
}

func UnpackJob(value []byte)(ret *Job,err error){
	var job *Job
	job = &Job{}
	err = json.Unmarshal(value, job)
	if err != nil {
		panic(err)
	}
	ret = job
	return
}

func ExtractJobName(jobKey string) string{
	return strings.TrimPrefix(jobKey,Job_Save_Dir)
}
func ExtractKillerName(jobKey string) string{
	return strings.TrimPrefix(jobKey,Job_Kill_Dir)
}

func ExtractWorkerIP(reKey string) string{
	return strings.TrimPrefix(reKey,IP_Save_Dir)
}

func BuildJobEvent(jobEventType int,job *Job) (jobEvent *JobEvent){
	return &JobEvent{
		JobEventType: jobEventType,
		Job: job,
	}
}

func BuildJobSchedulePlan(job *Job)(jobSchedulePlan *SchedulePlan,err error){
	 expr, err:= cronexpr.Parse(job.CronExpr)
	 if err != nil {
		return
	}
	jobSchedulePlan = &SchedulePlan{
		Job: job,
		Expr: expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

func BuildJobExecuteInfo(plan *SchedulePlan)(jobExecuteInfo *JobExecuteInfo,err error){
	now := time.Now()
	jobExecuteInfo = &JobExecuteInfo{
		Job: plan.Job,
		PlanTime: plan.NextTime,
		RealTime: now,
	}
	jobExecuteInfo.CalCtx,jobExecuteInfo.CancelFunc= context.WithCancel(context.TODO())
	return
}
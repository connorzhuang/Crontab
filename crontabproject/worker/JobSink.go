package worker

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"learngo2/crontab/common"
	"time"
)

type JobSink struct {
	client *mongo.Client
	collection *mongo.Collection
	jobLogChan chan *common.JobLog
	autoCommitChan chan *common.LogMap
}
var G_jobSink *JobSink
var logMap *common.LogMap

func (j *JobSink) PushJobLog(jobLog *common.JobLog){
	select {
	case j.jobLogChan <- jobLog:
	default:
	}

}
func (j *JobSink) SaveLogs(jobMap *common.LogMap)  {
	G_jobSink.collection.InsertMany(context.TODO(),jobMap.Logs)
}

func (j *JobSink) WriteLoop(){
	var (
		committimer *time.Timer //定时器
	)
	for{
		select {
		case jobLog := <- j.jobLogChan:
			if logMap == nil {
				logMap = &common.LogMap{}
				committimer	= time.AfterFunc(1*time.Second, func(logMap *common.LogMap) func(){
					return func() {
						j.autoCommitChan <- logMap
					}
				}(logMap),
				)
			}

			logMap.Logs =append(logMap.Logs,jobLog)
			if len(logMap.Logs) >= 100{
				j.SaveLogs(logMap)
				logMap = nil
				committimer.Stop()
			}

		case timeoutLog := <- j.autoCommitChan:
			if timeoutLog != logMap{
				continue
			}
			j.SaveLogs(timeoutLog)
			logMap = nil
		}
	}
}

func InitJobSink()(err error){

	clientOpts := options.Client().ApplyURI(G_config.MongodbUri)
	ctx, _ := context.WithTimeout(context.TODO(),time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)
	client, err := mongo.Connect(ctx, clientOpts)
	G_jobSink = &JobSink{
		client: client,
		collection: client.Database("cron").Collection("log"),
		jobLogChan: make(chan *common.JobLog,100),
		autoCommitChan: make(chan *common.LogMap,100),
	}

	go G_jobSink.WriteLoop()

	return
}

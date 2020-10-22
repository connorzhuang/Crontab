package master

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"learngo2/crontab/common"
	"time"
)

type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var G_logMgr *LogMgr



func InitLogMgr()(err error){
	clientOpts := options.Client().ApplyURI(G_config.MongodbUri)
	ctx, _ := context.WithTimeout(context.TODO(), time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)
	client, err := mongo.Connect(ctx, clientOpts)
	G_logMgr = &LogMgr{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}
func (l *LogMgr) ListLog(jobName string,skip int,limit int) (logArr []*common.JobLog,err error) {

	var jobLog *common.JobLog

	logArr = make([]*common.JobLog,0)
	filter := &common.JobLogFilter{JobName: jobName}
	logSort := &common.SortJobLogByStartTime{SortOrder: -1}

	cursor, err := l.logCollection.Find(context.TODO(), filter,options.Find().SetSort(logSort),
		options.Find().SetSkip(int64(skip)),
		options.Find().SetLimit(int64(limit)))
	if err != nil {
		return
	}
	defer cursor.Close(context.TODO())
	for cursor.Next(context.TODO()){
		jobLog =&common.JobLog{}
		if err := cursor.Decode(jobLog); err != nil {
			continue
		}
		logArr = append(logArr,jobLog)
	}
	return
}

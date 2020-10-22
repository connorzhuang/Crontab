package master

import (
	"encoding/json"
	"learngo2/crontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 任务的HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	// 单例对象
	G_apiServer *ApiServer
)

//保存任务接口
func handlerSave(w http.ResponseWriter,r *http.Request){
	var job common.Job

	//解析表单
	 err := r.ParseForm()
	 if err != nil {
		panic(err)
	}
	//获取表单中的字段
	postJob := r.PostForm.Get("job")
	//反序列化job
	err = json.Unmarshal([]byte(postJob), &job)
	if err != nil {
		panic(err)
	}

	oldJob, err := G_jobMgr.JobSave(&job)
	if err != nil {
		goto ERR
	}
	//成功应答
	if resp, err := common.BuildResponse(0, "success", oldJob);err == nil {
		w.Write(resp)
	}
	return
    ERR:
	if resp, err := common.BuildResponse(1, err.Error(), nil);err == nil {
		w.Write(resp)
	}

}
//删除任务接口
func handlerDelete(w http.ResponseWriter,r *http.Request){
	var name string
	//解析表单
	err := r.ParseForm()
	if err != nil {
		panic(err)
	}
	//获取jobName
	name = r.PostForm.Get("name")

	oldJob, err := G_jobMgr.JobDelete(name)
	if err != nil {
		goto ERR
	}

	if resp, err := common.BuildResponse(0, "success", oldJob);err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err := common.BuildResponse(1, err.Error(), nil);err == nil {
		w.Write(resp)
	}

}
//获取任务列表
func handlerJobList(w http.ResponseWriter,r *http.Request){
	jobList, err := G_jobMgr.JobList()
	if err != nil {
		goto ERR
	}

	if resp, err := common.BuildResponse(0, "success", jobList);err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err := common.BuildResponse(1, err.Error(), nil);err == nil {
		w.Write(resp)
	}

}
//杀死任务
func handlerJobKill(w http.ResponseWriter,r *http.Request){
	var name string
	err := r.ParseForm()
	if err != nil {
		panic(err)
	}
	name = r.PostForm.Get("name")
	err = G_jobMgr.JobKill(name)
	if err != nil {
		goto ERR
	}
	if resp, err := common.BuildResponse(0, "success", nil);err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err := common.BuildResponse(1, err.Error(), nil);err == nil {
		w.Write(resp)
	}
}
//查看任务日志
func handlerJobLog(w http.ResponseWriter,r *http.Request) {
	var (
		name string
		skipParam string
		limitParam string
		skip int
		limit int
	)
	err := r.ParseForm()
	if err != nil {
		panic(err)
	}
	name = r.Form.Get("name")
	skipParam = r.Form.Get("skip")
	limitParam = r.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam);err != nil {
		skip = 0
	}
	if limit,err =strconv.Atoi(limitParam);err != nil {
		limit = 20
	}
	logArr, err := G_logMgr.ListLog(name, skip, limit)
	if err != nil {
		goto ERR
	}
	if resp, err := common.BuildResponse(0, "success", logArr);err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err := common.BuildResponse(1, err.Error(), nil);err == nil {
		w.Write(resp)
	}
}

//获取worker健康节点

func handlerWorkerList(w http.ResponseWriter,r *http.Request){
	workerIpArrs, err := G_workerMgr.GetWorkerList()
	if err != nil {
		goto ERR
	}
	if resp, err := common.BuildResponse(0, "success", workerIpArrs);err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err := common.BuildResponse(1, err.Error(), nil);err == nil {
		w.Write(resp)
	}
}




// 初始化服务
func InitApiServer() (err error){


	// 配置路由
	mux := http.NewServeMux()
	mux.HandleFunc("/job/save", handlerSave)
	mux.HandleFunc("/job/delete",handlerDelete)
	mux.HandleFunc("/job/list",handlerJobList)
	mux.HandleFunc("/job/kill",handlerJobKill)
	mux.HandleFunc("/job/log",handlerJobLog)
	mux.HandleFunc("/worker/list",handlerWorkerList)
	//静态文件目录
	staticDir := http.Dir(G_config.WebRoot ) //  ./webroot
	handler := http.FileServer(staticDir)
	mux.Handle("/",http.StripPrefix("/",handler))

	// 启动TCP监听
	listener, err := net.Listen("tcp", ":" + strconv.Itoa(G_config.ApiPort))
	if err != nil {
		return
	}

	// 创建一个HTTP服务
	httpServer := &http.Server{
		ReadTimeout: time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler: mux,
	}

	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 启动了服务端
	go httpServer.Serve(listener)

	return
}

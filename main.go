package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

type Task struct {
	Uuid           string `json:"uuid"`
	Name           string `json:"name"`
	Endpoint       string `json:"endpoint"`
	RetryOnFailure bool   `json:"retryOnFailure"`
	MaxRetries     int    `json:"maxRetries"`
}

type Dag struct {
	Uuid  string `json:"uuid"`
	Name  string `json:"name"`
	Cron  string `json:"cron"`
	Tasks []Task `json:"tasks"`
}

type TaskExecution struct {
	Uuid     string    `json:"uuid"`
	Name     string    `json:"name"`
	DagUuid  string    `json:"dag_uuid"`
	TaskUuid string    `json:"task_uuid"`
	Attempts int       `json:"attempts"`
	Status   string    `json:"status"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Error    string    `json:"error"`
}

type DagExecution struct {
	Uuid                     string
	Name                     string
	DagUuid                  string
	Status                   string
	FailingTaskExecutionUuid string
	TaskExecutions           []TaskExecution
	Start                    time.Time
	End                      time.Time
}

func getMongoCredentials() (string, string) {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}
	mongoAPIEndpoint, mongoAPIKey := os.Getenv("MONGO_API_ENDPOINT"), os.Getenv("MONGO_API_KEY")
	if mongoAPIEndpoint == "" {
		panic("MONGO_API_ENDPOINT is not set")
	}
	if mongoAPIKey == "" {
		panic("MONGO_API_KEY is not set")
	}
	return mongoAPIEndpoint, mongoAPIKey
}

func mongoRequest(endpoint string, body []byte) []byte {
	mongoAPIEndpoint, mongoAPIKey := getMongoCredentials()
	client := &http.Client{}
	req, err := http.NewRequest("POST", mongoAPIEndpoint+endpoint, bytes.NewBuffer(body))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("authorization", "Bearer "+mongoAPIKey)
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return body
}

func taskExecutionToJson(taskExecution TaskExecution) string {
	json, err := json.Marshal(taskExecution)
	if err != nil {
		panic(err)
	}
	return string(json)
}

func dagExecutionToJson(dagExecution DagExecution) string {
	json, err := json.Marshal(dagExecution)
	if err != nil {
		panic(err)
	}
	return string(json)
}

func jsonToSchedule(body []byte) (array []Dag) {
	var schedule []Dag
	err := json.Unmarshal(body, &schedule)
	if err != nil {
		panic(err)
	}
	return schedule
}

func getScheduleFromDB() (array []Dag) {
	body := mongoRequest("find", []byte(`{"database":"scheduler","collection":"dags","query":{}}`))
	schedule := jsonToSchedule(body)
	return schedule
}

func updateTaskExecution(taskExecution TaskExecution) {
	database := "scheduler"
	collection := "task_executions"
	mongoRequest("upsert", []byte(`{"database":"`+database+`","collection":"`+collection+`","query":{"uuid":"`+taskExecution.Uuid+`"},"update":{"$set":`+taskExecutionToJson(taskExecution)+`}}`))
}

func updateDagExecution(dagExecution DagExecution) {
	database := "scheduler"
	collection := "dag_executions"
	mongoRequest("upsert", []byte(`{"database":"`+database+`","collection":"`+collection+`","query":{"uuid":"`+dagExecution.Uuid+`"},"update":{"$set":`+dagExecutionToJson(dagExecution)+`}}`))
}

func updateTaskExecutionError(taskExecution TaskExecution, error string) (taskExecutionUpdated TaskExecution) {
	taskExecution.Error = error
	taskExecution.Status = "failed"
	taskExecution.End = time.Now()
	updateTaskExecution(taskExecution)
	return taskExecution
}

func updateTaskExecutionSuccess(taskExecution TaskExecution) (taskExecutionUpdated TaskExecution) {
	taskExecution.Status = "success"
	taskExecution.End = time.Now()
	updateTaskExecution(taskExecution)
	return taskExecution
}

func updateDagExecutionSuccess(dagExecution DagExecution) (dagExecutionUpdated DagExecution) {
	dagExecution.Status = "success"
	dagExecution.End = time.Now()
	updateDagExecution(dagExecution)
	return dagExecution
}

func updateDagExecutionError(dagExecution DagExecution, failingTaskExecutionUuid string) (dagExecutionUpdated DagExecution) {
	dagExecution.Status = "failed"
	dagExecution.End = time.Now()
	dagExecution.FailingTaskExecutionUuid = failingTaskExecutionUuid
	updateDagExecution(dagExecution)
	return dagExecution
}

func executeTask(dag Dag, task Task) (taskExecution TaskExecution) {
	taskExecution = TaskExecution{
		Uuid:     uuid.New().String(),
		Name:     task.Name,
		DagUuid:  dag.Uuid,
		TaskUuid: task.Uuid,
		Attempts: 1,
		Status:   "running",
		Start:    time.Now(),
		End:      time.Now(),
		Error:    "",
	}
	updateTaskExecution(taskExecution)
	client := &http.Client{}
	req, err := http.NewRequest("GET", task.Endpoint, nil)

	if err != nil {
		return updateTaskExecutionError(taskExecution, err.Error())
	}
	resp, err := client.Do(req)
	if err != nil {
		return updateTaskExecutionError(taskExecution, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return updateTaskExecutionError(taskExecution, err.Error())
	}
	if resp.StatusCode != 200 {
		return updateTaskExecutionError(taskExecution, string(body))
	}
	return updateTaskExecutionSuccess(taskExecution)
}

func dagExecutionReplaceLastTaskExecution(dagExecution DagExecution, taskExecution TaskExecution) (dagExecutionUpdated DagExecution) {
	dagExecution.TaskExecutions[len(dagExecution.TaskExecutions)-1] = taskExecution
	updateDagExecution(dagExecution)
	return dagExecution
}

func executeDag(dag Dag) (dagExecution DagExecution) {
	dagExecution = DagExecution{
		Uuid:                     uuid.New().String(),
		Name:                     dag.Name,
		DagUuid:                  dag.Uuid,
		Status:                   "running",
		FailingTaskExecutionUuid: "",
		TaskExecutions:           []TaskExecution{},
		Start:                    time.Now(),
		End:                      time.Now(),
	}
	for _, task := range dag.Tasks {
		dagExecution.TaskExecutions = append(dagExecution.TaskExecutions, executeTask(dag, task))
		taskExecution := executeTask(dag, task)
		for taskExecution.Status == "failed" && task.RetryOnFailure && taskExecution.Attempts < task.MaxRetries {
			dagExecution = dagExecutionReplaceLastTaskExecution(dagExecution, taskExecution)
			taskExecution = executeTask(dag, task)
			if taskExecution.Status == "success" {
				break
			}
			if taskExecution.Status == "failed" {
				dagExecution = updateDagExecutionError(dagExecution, taskExecution.Uuid)
				return dagExecution
			} else {
				panic("Unknown task status")
			}
		}
		dagExecutionReplaceLastTaskExecution(dagExecution, taskExecution)
	}
	return updateDagExecutionSuccess(dagExecution)
}

func main() {
	fmt.Println("Starting scheduler")
	master := gocron.NewScheduler(time.UTC)
	scheduler := gocron.NewScheduler(time.UTC)
	schedule := getScheduleFromDB()
	for _, dag := range schedule {
		scheduler.CronWithSeconds(dag.Cron).Do(executeDag, dag)
	}
	master.Every(1).Seconds().Do(func() {
		scheduler.Clear()
		schedule = getScheduleFromDB()
		for _, dag := range schedule {
			scheduler.CronWithSeconds(dag.Cron).Do(executeDag, dag)
		}
	})
	master.StartAsync()
	scheduler.StartAsync()
	master.StartBlocking()
}

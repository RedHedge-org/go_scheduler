package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

const MAX_TASK_EXECUTION_RETRIES = 10

type Task struct {
	Uuid           string `json:"uuid"`
	Name           string `json:"name"`
	Endpoint       string `json:"endpoint"`
	RetryOnFailure bool   `json:"retry_on_failure"`
	MaxRetries     int    `json:"max_retries"`
}

type Dag struct {
	Uuid    string `json:"uuid"`
	Name    string `json:"name"`
	Cron    string `json:"cron"`
	Tasks   []Task `json:"tasks"`
	Enabled bool   `json:"enabled"`
}

type TaskExecution struct {
	Uuid     string `json:"uuid"`
	Name     string `json:"name"`
	DagUuid  string `json:"dag_uuid"`
	TaskUuid string `json:"task_uuid"`
	Attempts int    `json:"attempts"`
	Status   string `json:"status"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	Error    string `json:"error"`
}

type DagExecution struct {
	Uuid                     string          `json:"uuid"`
	Name                     string          `json:"name"`
	DagUuid                  string          `json:"dag_uuid"`
	Status                   string          `json:"status"`
	FailingTaskExecutionUuid string          `json:"failing_task_execution_uuid"`
	TaskExecutions           []TaskExecution `json:"task_executions"`
	Start                    int64           `json:"start"`
	End                      int64           `json:"end"`
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

func scheduleToJson(schedule []Dag) string {
	json, err := json.Marshal(schedule)
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

func getNewTaskExecution(dag Dag, task Task) (taskExecution TaskExecution) {
	taskExecution = TaskExecution{
		Uuid:     uuid.New().String(),
		Name:     task.Name,
		DagUuid:  dag.Uuid,
		TaskUuid: task.Uuid,
		Attempts: 0,
		Status:   "running",
		// start and end should be unix timestamps
		Start: time.Now().Unix(),
		End:   time.Now().Unix(),
		Error: "",
	}
	return taskExecution
}

func getNewDagExecution(dag Dag) (dagExecution DagExecution) {
	dagExecution = DagExecution{
		Uuid:                     uuid.New().String(),
		Name:                     dag.Name,
		DagUuid:                  dag.Uuid,
		Status:                   "running",
		FailingTaskExecutionUuid: "",
		TaskExecutions:           []TaskExecution{},
		Start:                    time.Now().Unix(),
		End:                      time.Now().Unix(),
	}
	updateDagExecution(dagExecution)
	return dagExecution
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

func getScheduleFromDB() (array []Dag) {
	body := mongoRequest("find", []byte(`{"database":"scheduler","collection":"dags","query":{"enabled":true}}`))
	schedule := jsonToSchedule(body)
	return schedule
}

func updateDagExecution(dagExecution DagExecution) {
	database := "scheduler"
	collection := "dag_executions"
	mongoRequest("upsert", []byte(`{"database":"`+database+`","collection":"`+collection+`","query":{"uuid":"`+dagExecution.Uuid+`"},"update":{"$set":`+dagExecutionToJson(dagExecution)+`}}`))
}

func updateTaskExecutionError(taskExecution TaskExecution, error string) (taskExecutionUpdated TaskExecution) {
	taskExecution.Error = error
	taskExecution.Status = "failed"
	taskExecution.End = time.Now().Unix()
	return taskExecution
}

func updateTaskExecutionSuccess(taskExecution TaskExecution) (taskExecutionUpdated TaskExecution) {
	taskExecution.Status = "success"
	taskExecution.End = time.Now().Unix()
	return taskExecution
}

func updateDagExecutionSuccess(dagExecution DagExecution) (dagExecutionUpdated DagExecution) {
	dagExecution.Status = "success"
	dagExecution.End = time.Now().Unix()
	updateDagExecution(dagExecution)
	return dagExecution
}

func dagExecutionReplaceLastTaskExecution(dagExecution DagExecution, taskExecution TaskExecution) (dagExecutionUpdated DagExecution) {
	dagExecution.TaskExecutions[len(dagExecution.TaskExecutions)-1] = taskExecution
	if dagExecution.Name == "" {
		panic("dagExecution name is empty")
	}
	updateDagExecution(dagExecution)
	return dagExecution
}

func updateDagExecutionError(dagExecution DagExecution, failingTaskExecution TaskExecution) (dagExecutionUpdated DagExecution) {
	dagExecution.Status = "failed"
	dagExecution.End = time.Now().Unix()
	dagExecution.FailingTaskExecutionUuid = failingTaskExecution.Uuid
	dagExecution = dagExecutionReplaceLastTaskExecution(dagExecution, failingTaskExecution)
	updateDagExecution(dagExecution)
	return dagExecution
}

func performHttpCall(method string, url string, headers map[string]string, body []byte) (response []byte) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		panic(err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		panic("http call failed with status code " + strconv.Itoa(resp.StatusCode))
	}
	defer resp.Body.Close()
	response, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return response
}

func executeTask(dag Dag, task Task, dagExecution DagExecution, taskExecution TaskExecution) (taskExecutionUpdated TaskExecution, dagExecutionUpdated DagExecution) {
	if len(dagExecution.TaskExecutions) > 0 && dagExecution.TaskExecutions[len(dagExecution.TaskExecutions)-1].Uuid == taskExecution.Uuid {
		dagExecution.TaskExecutions[len(dagExecution.TaskExecutions)-1] = taskExecution
	} else {
		dagExecution.TaskExecutions = append(dagExecution.TaskExecutions, taskExecution)
	}
	updateDagExecution(dagExecution)
	taskExecution.Attempts = taskExecution.Attempts + 1
	defer func() {
		if r := recover(); r != nil {
			taskExecutionUpdated = updateTaskExecutionError(taskExecution, fmt.Sprintf("%s", r))
			dagExecutionUpdated = updateDagExecutionError(dagExecution, taskExecution)
		}
	}()
	performHttpCall("GET", task.Endpoint, map[string]string{"Content-Type": "application/json"}, []byte(`{}`))
	return updateTaskExecutionSuccess(taskExecution), dagExecution
}

func executeDag(dag Dag) (dagExecution DagExecution) {
	dagExecution = getNewDagExecution(dag)
	for _, task := range dag.Tasks {
		taskExecution := getNewTaskExecution(dag, task)
		maxRetries := task.MaxRetries
		if task.MaxRetries > MAX_TASK_EXECUTION_RETRIES {
			maxRetries = MAX_TASK_EXECUTION_RETRIES
		}
		for {
			taskExecution, dagExecution = executeTask(dag, task, dagExecution, taskExecution)
			if taskExecution.Status == "success" {
				dagExecutionReplaceLastTaskExecution(dagExecution, taskExecution)
				break
			}
			if taskExecution.Status == "failed" {
				fmt.Println("Task failed", task.Name)
				dagExecution = updateDagExecutionError(dagExecution, taskExecution)
			}
			if !task.RetryOnFailure || taskExecution.Attempts >= maxRetries {
				return dagExecution
			}
			fmt.Println("Retrying task", task.Name)

		}
		if taskExecution.Status == "failed" {
			return updateDagExecutionError(dagExecution, taskExecution)
		}
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

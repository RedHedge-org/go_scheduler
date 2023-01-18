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

func updateTaskExecution(taskExecution TaskExecution) {
	body, err := json.Marshal(taskExecution)
	if err != nil {
		panic(err)
	}
	mongoRequest("update", body)

}

func getScheduleFromDB() (array []Dag) {
	body := mongoRequest("find", []byte(`{"database":"scheduler","collection":"dags","query":{}}`))
	schedule := jsonToSchedule(body)
	return schedule
}

func jsonToSchedule(body []byte) (array []Dag) {
	var schedule []Dag
	err := json.Unmarshal(body, &schedule)
	if err != nil {
		panic(err)
	}
	return schedule
}

type TaskExecution struct {
	Uuid     string
	DagUuid  string
	TaskUuid string
	Attempts int
	Status   string
	Start    time.Time
	End      time.Time
	Error    string
}

func executeTask(dag Dag, task Task) (taskExecution TaskExecution) {
	taskExecution = TaskExecution{
		Uuid:     "uuid",
		DagUuid:  dag.Uuid,
		TaskUuid: task.Uuid,
		Attempts: 1,
		Status:   "success",
		Start:    time.Now(),
		End:      time.Now(),
		Error:    "",
	}
	updateTaskExecution(taskExecution)
	client := &http.Client{}
	req, err := http.NewRequest("GET", task.Endpoint, nil)
	if err != nil {
		taskExecution.Status = "failed"
		taskExecution.End = time.Now()
		taskExecution.Error = err.Error()
		updateTaskExecution(taskExecution)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		taskExecution.Status = "failed"
		taskExecution.End = time.Now()
		taskExecution.Error = err.Error()
		updateTaskExecution(taskExecution)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		taskExecution.Status = "failed"
		taskExecution.End = time.Now()
		taskExecution.Error = err.Error()
		updateTaskExecution(taskExecution)
		return
	}
	if resp.StatusCode != 200 {
		taskExecution.Status = "failed"
		taskExecution.End = time.Now()
		taskExecution.Error = string(body)
		updateTaskExecution(taskExecution)
		return
	}
	taskExecution.Status = "success"
	taskExecution.End = time.Now()
	updateTaskExecution(taskExecution)
	return taskExecution
}

func executeDag(dag Dag) {
	fmt.Println("Executing dag: ", dag.Name)
	for _, task := range dag.Tasks {
		fmt.Println("	", task.Name)
		taskExecution := executeTask(dag, task)
		for taskExecution.Status == "failed" && task.RetryOnFailure && taskExecution.Attempts < task.MaxRetries {
			fmt.Println("	Retrying task: ", task.Name)
			taskExecution = executeTask(dag, task)
			if taskExecution.Status == "success" {
				break
			}
		}
	}

}

func main() {
	fmt.Println("Starting scheduler")
	s := gocron.NewScheduler(time.UTC)
	schedule := getScheduleFromDB()
	for _, dag := range schedule {
		s.CronWithSeconds(dag.Cron).Do(executeDag, dag)
	}
	s.StartAsync()
	s.StartBlocking()
}

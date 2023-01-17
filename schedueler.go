package main

import (
	"fmt"
	"os"

	"github.com/jasonlvhit/gocron"
	"github.com/joho/godotenv"
)

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



func task() {
	fmt.Println("I am running task.")
}


func main() {
	fmt.Println("Starting scheduler")
	mongoAPIEndpoint, mongoAPIKey := getMongoCredentials()
	fmt.Println(mongoAPIEndpoint, mongoAPIKey)

	gocron.Every(1).Second().Do(task)
	<-gocron.Start()
}
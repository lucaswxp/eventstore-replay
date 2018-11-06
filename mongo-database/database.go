package myDatabase

import (
	"context"
	"fmt"
	"os"

	"github.com/mongodb/mongo-go-driver/mongo"
)

var connection *mongo.Client

func ConnectMongo() *mongo.Client {
	if connection == nil {
		var err error
		var mongoHost string
		if mongoHost = os.Getenv("MONGO_HOST"); mongoHost == "" {
			mongoHost = "127.0.0.1"
		}

		connection, err = mongo.NewClient("mongodb://" + mongoHost + ":27017")
		if err != nil {
			fmt.Printf("ERRO conexão: %s", err)
			panic(err)
		}
		connection.Connect(context.Background())
		return connection
	} else {
		return connection
	}
}

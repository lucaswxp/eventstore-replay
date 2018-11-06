package myDatabase

import (
	"context"
	"fmt"
	"github.com/mongodb/mongo-go-driver/mongo"
)

var connection *mongo.Client
func ConnectMongo() (*mongo.Client){
	if connection == nil {
		var err error
		connection, err = mongo.NewClient("mongodb://localhost:27017")
		if err != nil {
			fmt.Printf("ERRO: %s", err)
			return nil
		}
		connection.Connect(context.Background())
		return connection
	} else{
		return connection
	}
}

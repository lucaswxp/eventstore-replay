package main

import (
	//"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/mongodb/mongo-go-driver/mongo"

	"strings"

	"context"
	"time"

	"./mongo-database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/streadway/amqp"
	"github.com/urfave/cli"
)

func main() {

	var routingKey string
	var prefix string
	var modifier string
	var aggregateType string
	var after string
	var aggregateID string

	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "routingKey",
			Value:       "projections",
			Usage:       "Default routing key, pass empty string for all keys",
			Destination: &routingKey,
		},
		cli.StringFlag{
			Name:        "prefix",
			Value:       "staging-stable",
			Usage:       "Prefix all events. Use 'staging' for sending to test env",
			Destination: &prefix,
		},
		cli.StringFlag{
			Name:        "id",
			Value:       "",
			Usage:       "Send single aggregate ID events",
			Destination: &aggregateID,
		},
		cli.StringFlag{
			Name:        "aggregateType",
			Value:       "",
			Usage:       "Send only aggregate events of certain type",
			Destination: &aggregateType,
		},
		cli.StringFlag{
			Name:        "after",
			Value:       "",
			Usage:       "Send only events after certain date, format: '2018-01-01 00:00:00'",
			Destination: &after,
		},
		cli.StringFlag{
			Name:        "modifier",
			Value:       "1",
			Usage:       "Set splice id",
			Destination: &modifier,
		},
	}

	app.Action = func(c *cli.Context) error {
		runApp(routingKey, prefix, aggregateID, aggregateType, after, modifier)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runApp(routingKey string, prefix string, aggregateID string, aggregateType string, after string, modifier string) {

	var mongoDatabase string

	var rabbitHost string
	var rabbitUser string
	var rabbitPassword string

	if mongoDatabase = os.Getenv("EVENTSTORE_MONGO_DATABASE"); mongoDatabase == "" {
		mongoDatabase = "eventstore"
	}

	if rabbitHost = os.Getenv("EVENTSTORE_RABBIT_HOST"); rabbitHost == "" {
		rabbitHost = "127.0.0.1"
	}
	if rabbitUser = os.Getenv("EVENTSTORE_RABBIT_USER"); rabbitUser == "" {
		rabbitUser = "guest"
	}
	if rabbitPassword = os.Getenv("EVENTSTORE_RABBIT_PASSWORD"); rabbitPassword == "" {
		rabbitPassword = "guest"
	}

	connection, err := amqp.Dial("amqp://" + rabbitUser + ":" + rabbitPassword + "@" + rabbitHost + ":5672/")
	defer connection.Close()
	channel, err := connection.Channel()

	if err != nil {
		panic(err.Error())
	}

	var query = bson.NewDocument()
	if aggregateID != "" {
		query = query.Append(bson.EC.String("id", aggregateID))
	}

	if aggregateType != "" {
		query = query.Append(bson.EC.String("events.0.aggregateType", aggregateType))
	}

	if after != "" {

	}

	c := myDatabase.ConnectMongo()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		var in64 = int64(i)
		go exec(routingKey, prefix, aggregateID, aggregateType, after, modifier, mongoDatabase, "events_"+strconv.FormatInt(in64, 16), query, c, channel)
	}
	wg.Wait()
}

func exec(routingKey string, prefix string, aggregateID string, aggregateType string, after string, modifier string, database string, collection string, query *bson.Document, mongo *mongo.Client, rabbit *amqp.Channel) {
	cursor, _ := mongo.Database(database).Collection(collection).Find(context.Background(), query)
	var items []Events
	for cursor.Next(context.Background()) {
		item := Events{}
		cursor.Decode(&item)
		items = append(items, item)
	}
	var counter = 0
	for _, obj := range items {
		var events = obj.Events
		for _, event := range events {
			counter++

			d := event.Data.(*bson.Document)

			var evv interface{}

			str := d.ToExtJSON((false))
			str = strings.Replace(str, "\n", "\\n", -1)
			str = strings.Replace(str, "\t", "\\t", -1)

			json.Unmarshal([]byte(str), &evv)

			ev := EventMessage{EventName: string(event.Fqn), EventData: evv}

			b, _ := json.Marshal(ev)

			data := amqp.Publishing{Body: b}
			id := string(event.AggregateId)
			modifierInt, _ := strconv.Atoi(modifier)

			bucket := id[0:modifierInt]

			var typeQueue string
			if routingKey == "" {
				typeQueue = "fanout"
			} else {
				typeQueue = "direct"
			}

			rabbit.Publish(prefix+"-"+string(event.AggregateType)+"-"+bucket+"-"+typeQueue, routingKey, false, false, data)

			fmt.Println("["+strconv.Itoa(counter)+"-"+bucket+"]", "sending", string(event.AggregateId))
		}
	}
}

type EventMessage struct {
	EventName string      `json:"eventName"`
	EventData interface{} `json:"eventData"`
}

type Events struct {
	Id     string  `json:"id" bson:"id"`
	Events []Event `json:"events" bson:"events"`
}

type Event struct {
	AggregateId   string      `json:"aggregateId" bson:"aggregateId"`
	AggregateType string      `json:"aggregateType" bson:"aggregateType"`
	Name          string      `json:"name" bson:"name"`
	Fqn           string      `json:"fqn" bson:"fqn"`
	Data          interface{} `json:"data" bson:"data"`
	EventId       string      `json:"eventId" bson:"eventId"`
	CreatedAt     time.Time   `bson:"createdAt"`
}

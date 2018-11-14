package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/mongodb/mongo-go-driver/mongo"

	//"strings"

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

var rabbitHost string
var rabbitUser string
var rabbitPassword string
var connection *amqp.Connection
var channel *amqp.Channel
var mutex = &sync.Mutex{}

func runApp(routingKey string, prefix string, aggregateID string, aggregateType string, after string, modifier string) {

	var mongoDatabase string



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

	var err error
	connection, err = amqp.Dial("amqp://" + rabbitUser + ":" + rabbitPassword + "@" + rabbitHost + ":5672/")
	defer connection.Close()
	channel, err = connection.Channel()

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
		go exec(routingKey, prefix, aggregateID, aggregateType, after, modifier, mongoDatabase, "events_"+strconv.FormatInt(in64, 16), query, c)
	}
	wg.Wait()
}

func normalizeDocument(document *bson.Document) *bson.Document{
	var tam =  document.Len()
	for i := 0; i < tam; i++ {
		el := document.ElementAt(uint(i))
		newEl := normalizeValue(el.Value())
		if newEl != nil {

			switch newEl.Type() {
			case bson.TypeString:
				document.Set(bson.EC.String(el.Key(), newEl.StringValue()))
			break
			case bson.TypeEmbeddedDocument:
				document.Set(bson.EC.SubDocument(el.Key(), newEl.MutableDocument()))
			break
			case bson.TypeArray:
				document.Set(bson.EC.Array(el.Key(), newEl.MutableArray()))
			break
			}
		}
	}

	return document
}

func normalizeValue(el *bson.Value) *bson.Value {
	switch el.Type() {
	case bson.TypeString:
		var title = el.StringValue()
		title = strings.Replace(title, "\n", "\\n", -1)
		title = strings.Replace(title, "\"", "\\\"", -1)
		title = strings.Replace(title, "\t", "\\t", -1)

		return bson.EC.String("", title).Value()
	case bson.TypeEmbeddedDocument:
		normalized := normalizeDocument(el.MutableDocument())
		return bson.EC.SubDocument("", normalized).Value()
	case bson.TypeArray:
		normalized := normalizeArray(el.MutableArray())
		return bson.EC.Array("", normalized).Value()
	case bson.TypeDateTime:
		var timestamp = time.Unix(el.DateTime() /1000, 0)

		return bson.EC.String("", timestamp.Format(time.RFC3339)).Value()
	}

	return nil
}

func normalizeArray(arr *bson.Array) *bson.Array {
	var tam =  arr.Len()
	for i := 0; i < tam; i++ {
		el,err := arr.Lookup(uint(i))

		if err != nil {
			panic(err)
		}

		newVal := normalizeValue(el)
		if newVal != nil {
			arr.Set(uint(i), newVal)
		}
	}
	return arr
}

func exec(routingKey string, prefix string, aggregateID string, aggregateType string, after string, modifier string, database string, collection string, query *bson.Document, mongo *mongo.Client) {

	cursor, _ := mongo.Database(database).Collection(collection).Find(context.Background(), query)
	var counter = 0
	for cursor.Next(context.Background()) {
		item := Events{}
		cursor.Decode(&item)

		var events = item.Events
		for _, event := range events {
			counter++

			d := event.Data.(*bson.Document)

			d = normalizeDocument(d)


			var evv interface{}

			str := d.ToExtJSON((false))


			errorUnmarshal := json.Unmarshal([]byte(str), &evv)
			if errorUnmarshal != nil {
				fmt.Printf("Error unmarshal: %s\n", errorUnmarshal)
				fmt.Printf("JSON bugado: %s\n", str)
			}

			ev := EventMessage{ID: event.EventId, AggregateType: event.AggregateType, EventName: string(event.Fqn), EventData: evv}

			b, errorMarshal := json.Marshal(ev)
			if errorMarshal != nil {
				fmt.Printf("Error marshal: %s\n", errorMarshal)
			}

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

			error := channel.Publish(prefix+"-"+string(event.AggregateType)+"-"+bucket+"-"+typeQueue, routingKey, false, false, data)
			if error != nil {

				connection.Close()

				mutex.Lock()
				fmt.Println("Erro rabbitmq: "+error.Error())
				connection, _ := amqp.Dial("amqp://" + rabbitUser + ":" + rabbitPassword + "@" + rabbitHost + ":5672/")
				channel, _ = connection.Channel()
				mutex.Unlock()

				channel.Publish(prefix+"-"+string(event.AggregateType)+"-"+bucket+"-"+typeQueue, routingKey, false, false, data)
				fmt.Println("["+strconv.Itoa(counter)+"-"+bucket+"]", "sending", string(event.AggregateId))
			} else {
				fmt.Println("["+strconv.Itoa(counter)+"-"+bucket+"]", "sending", string(event.AggregateId))
			}
		}
	}
}

type EventMessage struct {
	ID string      `json:"id"`
	AggregateType string      `json:"aggregateType"`
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

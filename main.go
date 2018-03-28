package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"github.com/urfave/cli"
)

func main() {

	var routingKey string
	var prefix string
	var aggregateType string
	var fqn string
	var after string
	var aggregateID string
	var pause int

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
			Value:       "",
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
			Name:        "fqn",
			Value:       "",
			Usage:       "Send single fqn stream of events",
			Destination: &fqn,
		},
		cli.IntFlag{
			Name:        "pause",
			Value:       3000,
			Usage:       "It will wait specified number of miliseconds between events for each aggregate root event stream",
			Destination: &pause,
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
	}

	app.Action = func(c *cli.Context) error {

		runApp(routingKey, prefix, aggregateID, aggregateType, after, fqn, pause)

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runApp(routingKey string, prefix string, aggregateID string, aggregateType string, after string, fqn string, pause int) {

	var mysqlHost string
	var mysqlUser string
	var mysqlPassword string
	var mysqlDatabase string

	var rabbitHost string
	var rabbitUser string
	var rabbitPassword string

	if mysqlHost = os.Getenv("EVENTSTORE_MYSQL_HOST"); mysqlHost == "" {
		mysqlHost = "localhost"
	}
	if mysqlUser = os.Getenv("EVENTSTORE_MYSQL_USER"); mysqlUser == "" {
		mysqlUser = "root"
	}
	if mysqlPassword = os.Getenv("EVENTSTORE_MYSQL_PASSWORD"); mysqlPassword == "" {
		mysqlPassword = ""
	}
	if mysqlDatabase = os.Getenv("EVENTSTORE_MYSQL_DATABASE"); mysqlDatabase == "" {
		mysqlDatabase = "eventstore"
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

	// Open mysql connection
	db, err := sql.Open("mysql", mysqlUser+":"+mysqlPassword+"@tcp("+mysqlHost+":3306)/"+mysqlDatabase)
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	// Open rabbitmq connection
	connection, err := amqp.Dial("amqp://" + rabbitUser + ":" + rabbitPassword + "@" + rabbitHost + ":5672/")

	defer connection.Close()

	channel, err := connection.Channel()

	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}

	// Execute the query
	query := "SELECT * FROM events"

	var conds []string
	var preparedValues []interface{}

	if aggregateID != "" {
		conds = append(conds, "aggregateId = ?")
		preparedValues = append(preparedValues, aggregateID)
	}

	if aggregateType != "" {
		conds = append(conds, "aggregateType = ?")
		preparedValues = append(preparedValues, aggregateType)
	}

	if fqn != "" {
		conds = append(conds, "fqn = ?")
		preparedValues = append(preparedValues, fqn)
	}

	if after != "" {
		conds = append(conds, "createdAt >= ?")
		preparedValues = append(preparedValues, after)
	}

	if len(conds) > 0 {
		query = (query + " WHERE " + strings.Join(conds, " AND "))
	}

	query = (query + " ORDER BY id ASC")

	fmt.Println(query)

	stmtIns, err := db.Prepare(query) // ? = placeholder
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	defer stmtIns.Close() // Close the statement when we leave main() / the program terminates

	rows, err := stmtIns.Query(preparedValues...)

	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	var typeQueue string
	if routingKey == "" {
		typeQueue = "fanout"
	} else {
		typeQueue = "direct"
	}

	// Fetch rows
	var counter = 0
	aggregateIndex := make(map[string]*Aggregate)

	var runningGoroutines = 0

	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		var evdata interface{}

		json.Unmarshal(values[5], &evdata)

		evEnvelope := EventData{EventName: string(values[4]), EventData: evdata}
		aggregateId := string(values[1])

		ev := EventMessage{AggregateType: string(values[2]), Fqn: string(values[4]), AggregateId: aggregateId, Data: &evEnvelope}

		_, exists := aggregateIndex[aggregateId]

		if !exists {
			aggregateIndex[aggregateId] = &Aggregate{Mutex: sync.RWMutex{}, Channel: make(chan *EventMessage, 1)}

			go (func(agg *Aggregate, aggregateId string) {
			loop:
				for {
					select {
					case elem := <-agg.Channel:

						b, _ := json.Marshal(elem.Data)

						data := amqp.Publishing{Body: b}

						channel.Publish(prefix+elem.AggregateType+"-"+typeQueue, routingKey, false, false, data)

						counter++
						fmt.Println("["+strconv.Itoa(counter)+"]", "sending", elem.AggregateId, elem.Fqn)

						if pause > 0 {
							time.Sleep(time.Millisecond * time.Duration(pause))
						}
					case <-time.After(15 * time.Second):
						close(agg.Channel)
						delete(aggregateIndex, aggregateId)
						runningGoroutines--
						break loop
					}
				}
			})(aggregateIndex[aggregateId], aggregateId)
			runningGoroutines++
		}

		go (func(ev *EventMessage) {
			aggregateIndex[aggregateId].Channel <- ev
		})(&ev)
	}

	if err = rows.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	for {
		if runningGoroutines == 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
}

type EventMessage struct {
	Data          *EventData `json:"data"`
	AggregateType string     `json:"aggregateType"`
	Fqn           string     `json:"fqn"`
	AggregateId   string     `json:"aggregateId"`
}

type EventData struct {
	EventName string      `json:"eventName"`
	EventData interface{} `json:"eventData"`
}

type Aggregate struct {
	Channel chan *EventMessage
	Mutex   sync.RWMutex
}

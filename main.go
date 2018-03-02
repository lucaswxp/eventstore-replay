package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"github.com/urfave/cli"
)

func main() {

	var routingKey string
	var prefix string
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

		runApp(routingKey, prefix, aggregateID, aggregateType, after)

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runApp(routingKey string, prefix string, aggregateID string, aggregateType string, after string) {

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
	for rows.Next() {
		counter++
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		var evdata interface{}

		json.Unmarshal(values[5], &evdata)

		ev := EventMessage{EventName: string(values[4]), EventData: evdata}
		b, _ := json.Marshal(ev)

		data := amqp.Publishing{Body: b}

		channel.Publish(prefix+string(values[2])+"-"+typeQueue, routingKey, false, false, data)

		fmt.Println("["+strconv.Itoa(counter)+"]", "sending", string(values[1]), string(values[4]))
	}
	if err = rows.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
}

type EventMessage struct {
	EventName string      `json:"eventName"`
	EventData interface{} `json:"eventData"`
}

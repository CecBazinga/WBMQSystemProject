package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"os"
)

func initDBClient() *dynamodb.DynamoDB {

	if dynamoDBSession == nil {

		sess := session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		}))

		dynamoDBSession = dynamodb.New(sess)
	}

	return dynamoDBSession
}

//add bot to DB
func AddDBBot(bot Bot) {
	client := initDBClient()
	av, err := dynamodbattribute.MarshalMap(bot)
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String("bots"),
	}
	_, err = client.PutItem(input)
	if err != nil {
		fmt.Println(err.Error())
	}
}

// return the bot list in db if any
func GetDBBots() ([]Bot, error) {
	client := initDBClient()
	params := &dynamodb.ScanInput{
		TableName: aws.String("bots"),
	}

	fmt.Println("Params are", params)
	result, err := client.Scan(params)
	if err != nil {
		fmt.Println("Step 1")
		fmt.Println(err)
		return nil, err
	}

	var botslist = []Bot{}
	for _, i := range result.Items {
		bot := Bot{}
		err = dynamodbattribute.UnmarshalMap(i, &bot)
		if err != nil {
			fmt.Println("Step 2")
			fmt.Println(err)
			return nil, err
		}
		botslist = append(botslist, bot)
	}
	return botslist, nil
}

// return the list of botIds and their own messages which need to be retransmitted
func GetResilienceEntries() ([]resilienceEntry, error) {
	client := initDBClient()
	params := &dynamodb.ScanInput{
		TableName: aws.String("resilience"),
	}

	fmt.Println("Params are", params)
	result, err := client.Scan(params)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var resilienceList = []resilienceEntry{}
	for _, i := range result.Items {
		entry := resilienceEntry{}
		err = dynamodbattribute.UnmarshalMap(i, &entry)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		resilienceList = append(resilienceList, entry)
	}
	return resilienceList, nil
}

// return the list of sensor publish request which need to be retransmitted
func GetRequestEntries() ([]Sensor, error) {
	client := initDBClient()
	params := &dynamodb.ScanInput{
		TableName: aws.String("sensorsRequest"),
	}

	fmt.Println("Params are", params)
	result, err := client.Scan(params)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var requestList = []Sensor{}
	for _, i := range result.Items {
		entry := Sensor{}
		err = dynamodbattribute.UnmarshalMap(i, &entry)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		requestList = append(requestList, entry)
	}

	return requestList, nil
}

//add sensor to DB
func AddDBSensorRequest(sensor Sensor) {
	client := initDBClient()
	av, err := dynamodbattribute.MarshalMap(sensor)
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String("sensorsRequest"),
	}
	_, err = client.PutItem(input)
	if err != nil {
		fmt.Println(err.Error())
	}
}

//add every bot id and the message to be sent to this bot in resilience DynamoDb table
func writeBotIdsAndMessage(botsArray []Bot, sensor Sensor) {

	client := initDBClient()

	fmt.Println("BOT IDS ARRAY SIZE IS : \n")
	fmt.Println(len(botsArray))
	fmt.Println("MESSAGE IS : \n")
	fmt.Println(sensor.Message)

	for _, bot := range botsArray {

		fmt.Println("Id number : " + bot.Id)
		var item resilienceEntry
		item.Id = bot.Id + sensor.Id
		item.Message = sensor.Message

		av, err := dynamodbattribute.MarshalMap(item)
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String("resilience"),
		}
		_, err = client.PutItem(input)
		if err != nil {
			fmt.Println("THERE WE GO !!!")
			fmt.Println(err.Error())
		}
	}
}

//removes the entry (botId,message) from resilience table if bot identified by botId received correctly message
//and answered with an ack to te sending goroutine
func removeResilienceEntry(botId string, message string, sensor string) {

	client := initDBClient()
	id := botId + sensor
	thisMessage := message
	thisSensor := sensor

	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
			"message": {
				S: aws.String(thisMessage),
			},
		},
		TableName: aws.String("resilience"),
	}

	_, err := client.DeleteItem(params)
	if err != nil {
		fmt.Println("Got error calling DeleteItem on bot : " + botId + " and with message : " + message +
			"from sensor : " + thisSensor + "\n")
		fmt.Println(err.Error())

	} else {

		fmt.Println("Deleted resielience entry : bot = " + botId + "  and message = " + message +
			"from sensor : " + thisSensor + "\n")
	}
}

//removes the entry (botId,message) from resilience table if bot identified by botId received correctly message
//and answered with an ack to te sending goroutine
func removePubRequest(sensorId string, message string) {

	client := initDBClient()
	id := sensorId
	thisMessage := message

	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
			"msg": {
				S: aws.String(thisMessage),
			},
		},
		TableName: aws.String("sensorsRequest"),
	}

	_, err := client.DeleteItem(params)
	if err != nil {
		fmt.Println("Got error calling DeleteItem on PubRequest on sensor : " + id + " and with message : " + thisMessage + "\n")
		fmt.Println(err.Error())

	} else {
		fmt.Println("Deleted sensorsRequest entry : sensor  = " + id + "  and message = " + thisMessage + "\n")
	}
}

//func that checks if there are tables in dynamoDB
func ExistingTables() (int, error) {

	// create the input configuration instance
	input := &dynamodb.ListTablesInput{}

	client := initDBClient()

	// Get the list of tables
	result, err := client.ListTables(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return -1, err
	}

	return len(result.TableNames), nil

}

func removeBot(id string) error {

	client := initDBClient()
	fmt.Println("id bot: ", id)

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String("bots"),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: &id,
			},
		},
	}
	var err error
	_, err = client.DeleteItem(input)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	fmt.Println("---- Bot " + id + " was successfully removed ")
	return nil
}

//creates new Bots and Sensors tables
func createTables() {

	client := initDBClient()

	// Create table bots
	tableNameBots := "bots"

	inputBots := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},

		TableName: aws.String(tableNameBots),
	}

	_, err := client.CreateTable(inputBots)
	if err != nil {
		fmt.Println("Got error calling CreateTable:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fmt.Println("Created the table", tableNameBots)

	// Create table sensorsRequest
	tableSensorsRequest := "sensorsRequest"

	inputSensors := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("msg"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("msg"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},

		TableName: aws.String(tableSensorsRequest),
	}

	_, err2 := client.CreateTable(inputSensors)
	if err2 != nil {
		fmt.Println("Got error calling CreateTable:")
		fmt.Println(err2.Error())
		os.Exit(1)
	}

	fmt.Println("Created the table", tableSensorsRequest)

	// Create table resilience
	tableNameResilience := "resilience"

	inputResilience := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("message"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("message"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},

		TableName: aws.String(tableNameResilience),
	}

	_, err3 := client.CreateTable(inputResilience)
	if err3 != nil {
		fmt.Println("Got error calling CreateTable:")
		fmt.Println(err3.Error())
		os.Exit(1)
	}

	fmt.Println("Created the table", tableNameResilience)

}

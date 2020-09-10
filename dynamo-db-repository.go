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

	if dynamoDBSession == nil{

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

	fmt.Println("Params are" , params)
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

//add sensor to DB
func AddDBSensor(sensor Sensor) {
	client := initDBClient()
	av, err := dynamodbattribute.MarshalMap(sensor)
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String("sensors"),
	}
	_, err = client.PutItem(input)
	if err != nil {
		fmt.Println(err.Error())
	}
}

//returns the sensor list in db if any
func GetDBSensors() ([]Sensor, error) {
	client := initDBClient()
	params := &dynamodb.ScanInput{
		TableName: aws.String("sensors"),
	}
	result, err := client.Scan(params)
	if err != nil {
		return nil, err
	}

	var sensorslist = []Sensor{}
	for _, i := range result.Items {
		sensor := Sensor{}
		err = dynamodbattribute.UnmarshalMap(i, &sensor)
		if err != nil {
			return nil, err
		}
		sensorslist = append(sensorslist, sensor)
	}
	return sensorslist, nil
}

//func that checks if there are tables in dynamoDB
func ExistingTables() (int , error){

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
		return -1,err
	}

	return len(result.TableNames),nil

}


//creates new Bots and Sensors tables
func createTables() {

	client := initDBClient()

	// Create table bots
	tableNameBots := "bots"
	tableNameSensors := "sensors"

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

	inputSensors := &dynamodb.CreateTableInput{
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

		TableName: aws.String(tableNameSensors),
	}

	_, err2 := client.CreateTable(inputSensors)
	if err2 != nil {
		fmt.Println("Got error calling CreateTable:")
		fmt.Println(err2.Error())
		os.Exit(1)
	}

	fmt.Println("Created the table", tableNameSensors)



}
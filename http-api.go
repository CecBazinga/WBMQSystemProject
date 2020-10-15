package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/gorilla/mux"
	"github.com/lithammer/shortuuid"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Result
type Result struct {
	Id     string      `json:"id"`
	Data   interface{} `json:"data"`
	Topic  string      `json:"topic"`
	Sector string      `json:"sector"`
}

// Ping
type Ping struct {
	CtxStatus string    `json:"status"`
	TotBot    int       `json:"totbot"`
	TotSens   int       `json:"totsens"`
	Timestamp time.Time `json:"timestamp"`
}

// Bot
type Bot struct {
	Id            string `json:"id"`
	CurrentSector string `json:"current_sector"`
	Topic         string `json:"topic"`
	IpAddress     string `json:"ipaddr"`
}

// Sensor
type Sensor struct {
	Id            string `json:"id"`
	Message       string `json:"msg"`
	CurrentSector string `json:"current_sector"`
	Type          string `json:"type"`
	Pbrtx         bool   `json:"pbrtx"`
}

//resilience entry
type resilienceEntry struct {
	Id      string `json:"id"`
	Message string `json:"message"`
}

var bots []Bot
var topics []string
var contextLock = false
var dynamoDBSession *dynamodb.DynamoDB = nil
var sensorRequest sync.WaitGroup
var resilienceLock sync.WaitGroup

func main() {
	router := mux.NewRouter()

	checkCli()
	tablesNumber, err := ExistingTables()
	if err != nil {
		panic(err)
	}
	if tablesNumber == 0 {
		//create new tables
		createTables()
		time.Sleep(10 * time.Second)
	}

	checkDynamoBotsCache()

	fmt.Println("System started working \n")

	//get lock because i want to be sure no other function works on db in this moment, to get a copy of system's pre
	//crash state
	resilienceLock.Add(1)
	go checkResilience()
	fmt.Println("Waiting for checkresilience to read from DB")
	resilienceLock.Wait()

	fmt.Println("End of waiting for checkresilience to read from DB")

	initTopics()

	router.HandleFunc("/status", heartBeatMonitoring).Methods("GET")

	router.HandleFunc("/unsubscribeBot", unsubscribeBot).Methods("POST")
	router.HandleFunc("/bot", spawnBot).Methods("POST")

	router.HandleFunc("/sensor", spawnSensor).Methods("POST")

	// linea standard per mettere in ascolto l'app. TODO controllo d'errore
	go func() {
		log.Fatal(http.ListenAndServe(":5000", router))
	}()

	//cicla sulla lista di richieste di tipo sensorRequests

	for {

		if len(eb.sensorsRequest) > 0 {

			request := eb.sensorsRequest[0]
			fmt.Println("THE MESSAGE IS : " + request.Message + "\n")
			sensorRequest.Add(1)

			go func(request Sensor) {

				fmt.Println("LEN inside main go func: -----------------------------")
				fmt.Println(len(eb.subscribersCtx))
				fmt.Println(eb.subscribersCtx)
				fmt.Println("------------------------------------------------------")

				myRequest := request
				sensorRequest.Done()
				eb.Publish(myRequest)

			}(request)

			sensorRequest.Wait()
			eb.lockQueue.Lock()

			eb.sensorsRequest = append(eb.sensorsRequest[:0], eb.sensorsRequest[1:]...)

			eb.lockQueue.Unlock()

		}

	}
}

// STATIC OBJECT IN THE SYSTEM
func initTopics() {
	topics = make([]string, 0)
	topics = append(topics,
		"temperature",
		"humidity",
		"motion")
}

//check for first element inserted by command-line to create a context/non context aware environment
func checkCli() {
	if len(os.Args) > 1 {
		arg := os.Args[1]
		if arg == "ctx" {
			contextLock = true
		} else {
			fmt.Println("Wrong argument inserted!")
		}
	}
}

// function to ping the application
func heartBeatMonitoring(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var pingNow Ping
	pingNow.Timestamp = time.Now()
	if contextLock == true {
		pingNow.CtxStatus = "alivectx"
	} else {
		pingNow.CtxStatus = "alive"
	}
	pingNow.TotBot = len(bots)
	pingNow.TotSens = len(eb.sensorsRequest)
	json.NewEncoder(w).Encode(pingNow)
}

//retrieve bots state from DB if any robot is found and subscribe them to their topics
func checkDynamoBotsCache() {
	res, err := GetDBBots()

	if err != nil {
		panic(err)
	}
	for _, i := range res {
		bots = append(bots, i)
		eb.Subscribe(i)
	}
}

//spawns a new sensor with given values
func spawnSensor(w http.ResponseWriter, r *http.Request) {

	var newSensor Sensor

	//Genero un numero casuale tra 1 e 10 e se x>7 allora rispondo

	json.NewDecoder(r.Body).Decode(&newSensor)

	//check if sensor already in system
	if newSensor.Id != "" {
		fmt.Println("New message from a sensor alredy in the system with id : " + newSensor.Id)
	} else {
		newSensor.Id = shortuuid.New()
		//sensorsRequest = append(sensorsRequest, newSensor)
		//AddDBSensorRequest(newSensor)
	}

	var msg = newSensor.Message
	var ack = "Ack on message : " + msg + " on sensor :" + newSensor.Id

	//check if message is a new message or a retransmission
	//if PiggyBagRetransmission is true it means that message ahd already been transmitted so i do no op but ack
	if newSensor.Pbrtx {

		fmt.Println("FACCIO FINTA DI ESEGUIRE IL SERVIZIO !")

	} else if !newSensor.Pbrtx {

		AddDBSensorRequest(newSensor)
		eb.lockQueue.Lock()
		eb.sensorsRequest = append(eb.sensorsRequest, newSensor)
		eb.lockQueue.Unlock()

	}
	newSensor.Message = ack
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newSensor)
}

//spawns a new bot with given values
func spawnBot(w http.ResponseWriter, r *http.Request) {
	var newBot Bot
	json.NewDecoder(r.Body).Decode(&newBot)
	newBot.Id = shortuuid.New()
	bots = append(bots, newBot)

	AddDBBot(newBot)
	eb.Subscribe(newBot)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newBot)
}

func checkResilience() {

	resilience, err := GetResilienceEntries()
	if err != nil {
		panic(err)
	}

	requestSlice, err1 := GetRequestEntries()
	if err1 != nil {
		panic(err)
	}

	//once i got the system's state before crash i can release lock for main to gon on and listen and serve new requests
	//while i serve the older ones too
	resilienceLock.Done()

	fmt.Println("RESILIENCE CHECK SIZE IS : \n")
	fmt.Println(len(resilience))

	fmt.Println("REQUEST CHECK SIZE IS : \n")
	fmt.Println(len(requestSlice))

	var mainWg sync.WaitGroup

	//for every bot there is a subroutine which sends the message to it and awaits for its ack
	for _, requestItem := range requestSlice {

		mainWg.Add(1)

		go func(mySensor Sensor) {

			var wg sync.WaitGroup

			myRequestItem := requestItem
			requestResilienceEntries := []resilienceEntry{}

			var sensor Sensor
			sensor.Id = myRequestItem.Id
			sensor.Message = myRequestItem.Message
			sensor.Type = myRequestItem.Type
			sensor.Pbrtx = myRequestItem.Pbrtx
			sensor.CurrentSector = myRequestItem.CurrentSector

			//for every request creates the list of its own resilience entries
			for _, resilienceItem := range resilience {

				if resilienceItem.Message == sensor.Message && strings.Contains(resilienceItem.Id, sensor.Id) {

					requestResilienceEntries = append(requestResilienceEntries, resilienceItem)
				}
			}

			if len(requestResilienceEntries) > 0 {

				//retransmit the request to every entry
				for _, resilienceItem := range requestResilienceEntries {

					myBot := findBotbyId(strings.ReplaceAll(resilienceItem.Id, myRequestItem.Id, ""))

					if myBot.Id == "" {
						fmt.Println("NO BOT ASSOCIATED WITH THIS RESILIENCE ENTRY : SOMETHING WRONG \n")
						fmt.Println(strings.ReplaceAll(resilienceItem.Id, myRequestItem.Id, "") + "\n")

					} else if myBot.Id != "" {

						wg.Add(1)

						go publishImplementation(myBot, sensor, &wg)
					}

				}

				//awaits for all subroutines to end with an ack
				wg.Wait()
			}

			removePubRequest(sensor.Id, sensor.Message)

			mainWg.Done()

		}(requestItem)

	}

	mainWg.Wait()
}

func findBotbyId(id string) Bot {

	for _, bot := range bots {

		if bot.Id == id {

			return bot
		}
	}
	var emptyBot Bot

	return emptyBot
}

//unsubscribes bot with a given Id from current topic
func unsubscribeBot(w http.ResponseWriter, r *http.Request) {

	var newBot Bot
	json.NewDecoder(r.Body).Decode(&newBot)

	eb.Unsubscribe(newBot)
	for k, bot := range bots {

		if bot.Id == newBot.Id {

			bots = append(bots[:k], bots[k+1:]...)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	// send back some ack
	json.NewEncoder(w).Encode(newBot)
}



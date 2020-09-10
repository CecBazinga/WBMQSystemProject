package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/gorilla/mux"
	"github.com/lithammer/shortuuid"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
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
	Timestamp time.Time `json:"timestamp"`
}

// Bot
type Bot struct {
	Id            string `json:"id"`
	CurrentSector string `json:"current_sector"`
	Topic         string `json:"topic"`
}

// Sensor
type Sensor struct {
	Id            string `json:"id"`
	CurrentSector string `json:"current_sector"`
	Type          string `json:"type"`
}

var bots []Bot
var sensors []Sensor
var warehouses []string
var topics []string
var ch []chan DataEvent
var contextLock = false
var results []Result
var dynamoDBSession *dynamodb.DynamoDB = nil

func main() {
	router := mux.NewRouter()

	checkCli()
	tablesNumber,err := ExistingTables()
	if err != nil {
		panic(err)
	}
	if tablesNumber == 0 {
		//create new tables
		createTables()
		time.Sleep(10*time.Second)
	}
	checkDynamoBotsCache()
	checkDynamoSensorsCache()
	initWarehouseSpaces()
	initTopics()

	router.HandleFunc("/results", getResults).Methods("GET")

	router.HandleFunc("/status", heartBeatMonitoring).Methods("GET")
	router.HandleFunc("/switch", switchContext).Methods("POST")

	// TODO KILL BOT ROUTE
	router.HandleFunc("/bot", spawnBot).Methods("POST")
	router.HandleFunc("/bot/{num}", spawnBotRand).Methods("POST")
	router.HandleFunc("/bot", listAllBots).Methods("GET")

	// TODO KILL BOT SENSOR
	router.HandleFunc("/sensor", spawnSensor).Methods("POST")
	router.HandleFunc("/sensor/{num}", spawnSensorRand).Methods("POST")
	router.HandleFunc("/sensor", listAllSensors).Methods("GET")

	// linea standard per mettere in ascolto l'app. TODO controllo d'errore
	go func() {
		log.Fatal(http.ListenAndServe(":5000", router))
	}()

	// Broker checker ... IDMapToChannel is a bit tricky way to do things ...
	// TODO Need to chek for at-least-one semantic!! (bugs)

	for {
		if len(ch) > 0 {
			for i, c := range ch {
				select {
				case d := <-c:
					results = append(results, Result{
						Id:     IDMapToChannel[i],
						Data:   d.Data,
						Topic:  d.Topic,
						Sector: SectorMapping[i],
					})
					//go printDataEvent(IDMapToChannel[i], d)
				default:
					continue
				}
			}
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

// STATIC OBJECT IN THE SYSTEM
func initWarehouseSpaces() {
	warehouses = make([]string, 0)
	warehouses = append(warehouses,
		"a1",
		"a2",
		"a3",
		"a4",
		"b1",
		"b2",
		"c1",
		"c2",
		"c3",
		"c4")
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

// MAYBE FUTURE IMPLEMENTATION?
func switchContext(w http.ResponseWriter, r *http.Request) {
	if contextLock == true {
		contextLock = false
	} else {
		contextLock = true
	}
}


// function to ping the application
func heartBeatMonitoring(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var pingNow Ping
	pingNow.Timestamp = time.Now()
	if contextLock == true {
		pingNow.CtxStatus = "alivectx"
		json.NewEncoder(w).Encode(pingNow)
	} else {
		pingNow.CtxStatus = "alive"
		json.NewEncoder(w).Encode(pingNow)
	}
}

// retrieve sensor state from DB and initializes any sensor to spawn data (if any sensor is found)
func checkDynamoSensorsCache() {
	res, err := GetDBSensors()
	if err != nil {
		panic(err)
	}
	for _, i := range res {
		sensors = append(sensors, i)
		go func() {
			publishTo(i)
		}()
	}
}

//retrieve bots state from DB if any robot is found and subscribe them to their topics
func checkDynamoBotsCache() {
	res, err := GetDBBots()

	//fmt.Println(res)
	//fmt.Println(err)

	if err != nil {
		panic(err)
	}
	for _, i := range res {
		bots = append(bots, i)
		chn := make(chan DataEvent)
		ch = append(ch, chn)
		eb.Subscribe(i, chn)
	}
}

//function to get first 50 results everytime
func getResults(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// Need to set a ceil value for the results to send ... 50 now
	resultsTrunc := results[:50] // [ [1,2, ... ,50], 51, 52, 53 ... ]
	json.NewEncoder(w).Encode(resultsTrunc)
	results = results[50:] // [50, 51, 52, 53 ... ]
}


//spawna un sensore randomico o un numero randomico di sensori?
func spawnSensorRand(w http.ResponseWriter, r *http.Request) {
	var num = mux.Vars(r)["num"]
	totnum, err := strconv.Atoi(num)
	//json.NewDecoder(r.Body).Decode(&newBot)
	if err != nil {
		// there was an error
		w.WriteHeader(400)
		w.Write([]byte("ID could not be converted to integer"))
		return
	}
	for i := 0; i < totnum; i++ {
		var newSensor Sensor
		newSensor.Id = shortuuid.New()
		newSensor.CurrentSector = warehouses[rand.Intn(len(warehouses))]
		newSensor.Type = topics[rand.Intn(len(topics))]
		sensors = append(sensors, newSensor)
		AddDBSensor(newSensor)
		go func() {
			publishTo(newSensor)
		}()
	}

	// make channel

	// what to return? 200 ? dunno
	//w.Header().Set("Content-Type", "application/json")
	//json.NewEncoder(w).Encode(newSensor)
}

//spawns a new sensor with given values
func spawnSensor(w http.ResponseWriter, r *http.Request) {
	var newSensor Sensor
	json.NewDecoder(r.Body).Decode(&newSensor)
	newSensor.Id = shortuuid.New()
	sensors = append(sensors, newSensor)

	// make channel
	AddDBSensor(newSensor)
	go func() {
		publishTo(newSensor)
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newSensor)
}


func spawnBotRand(w http.ResponseWriter, r *http.Request) {
	var num = mux.Vars(r)["num"]
	totnum, err := strconv.Atoi(num)
	//json.NewDecoder(r.Body).Decode(&newBot)
	if err != nil {
		// there was an error
		w.WriteHeader(400)
		w.Write([]byte("ID could not be converted to integer"))
		return
	}
	for i := 0; i < totnum; i++ {
		var newBot Bot
		newBot.Id = shortuuid.New()
		newBot.CurrentSector = warehouses[rand.Intn(len(warehouses))]
		newBot.Topic = topics[rand.Intn(len(topics))]
		bots = append(bots, newBot)
		AddDBBot(newBot)
		chn := make(chan DataEvent)
		ch = append(ch, chn)
		eb.Subscribe(newBot, chn)
	}

	// make channel

	// what to return? 200 ? dunno
	//w.Header().Set("Content-Type", "application/json")
	//json.NewEncoder(w).Encode(newBot)
}

//spawns a new bot with given values
func spawnBot(w http.ResponseWriter, r *http.Request) {
	var newBot Bot
	json.NewDecoder(r.Body).Decode(&newBot)
	newBot.Id = shortuuid.New()
	bots = append(bots, newBot)

	// make channel
	AddDBBot(newBot)
	chn := make(chan DataEvent)
	ch = append(ch, chn)
	eb.Subscribe(newBot, chn)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newBot)
}

//send bots list as response
func listAllBots(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// Fillo la response e mando la lista di bots
	json.NewEncoder(w).Encode(bots)
}

//send sensors list as response
func listAllSensors(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// Fillo la response e mando la lista di bots
	json.NewEncoder(w).Encode(sensors)
}

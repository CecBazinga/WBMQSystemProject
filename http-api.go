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
var ch []chan DataEvent
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


	//checkDynamoSensorsCache()

	//get lock because i want to be sure no other function works on db in this moment, to get a copy of system's pre
	//crash state
	resilienceLock.Add(1)
	go checkResilience()
	fmt.Println("Waiting for checkresilience to read from DB")
	resilienceLock.Wait()

	fmt.Println("End of waiting for checkresilience to read from DB")

	initTopics()

	router.HandleFunc("/status", heartBeatMonitoring).Methods("GET")
	router.HandleFunc("/switch", switchContext).Methods("POST")

	// TODO KILL BOT ROUTE
	router.HandleFunc("/unsubscribeBot", unsubscribeBot).Methods("POST")
	//router.HandleFunc("/unsubscribeRandomBot/{num}", unsubRandomBot).Methods("POST")
	router.HandleFunc("/killSingleBot", killBot).Methods("POST")
	//router.HandleFunc("/killRandomBot/{num}", killRandomBot).Methods("POST")
	router.HandleFunc("/bot", spawnBot).Methods("POST")

	// TODO KILL BOT SENSOR
	//router.HandleFunc("/killRandomSensor/{num}", killRandomSensor).Methods("POST")
	router.HandleFunc("/killSingleSensor", killSensor).Methods("POST")
	router.HandleFunc("/sensor", spawnSensor).Methods("POST")

	// linea standard per mettere in ascolto l'app. TODO controllo d'errore
	go func() {
		log.Fatal(http.ListenAndServe(":5000", router))
	}()

	//cicla sulla lista di richieste di tipo sensorRequests

	for {

		if len(eb.sensorsRequest) > 0 {

			request := eb.sensorsRequest[0]
			fmt.Println("THE MESSAGE IS : " + request.Message +"\n")
			sensorRequest.Add(1)

			go func(request Sensor) {

				myRequest := request
				sensorRequest.Done()
				eb.Publish(myRequest)


			}(request)

			sensorRequest.Wait()
			eb.lockQueue.Lock()

			eb.sensorsRequest = append(eb.sensorsRequest[:0],eb.sensorsRequest[1:]...)

			eb.lockQueue.Unlock()

		}

	}
	//endlessWait.Wait()
}

func killBot(w http.ResponseWriter, r *http.Request) {

	var id string

	var check bool

	json.NewDecoder(r.Body).Decode(&id)
	fmt.Println("id bot: ", id)

	//var err bool
	check, _ = removeBot(id)

	if check == false {
		fmt.Println("--- Error: bot was not killed")
	} else {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(id)
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

// MAYBE FUTURE IMPLEMENTATION? Figo da implementare switch context a runtime?
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
		eb.sensorsRequest = append(eb.sensorsRequest, i)
		go func(sensor Sensor) {
			eb.Publish(sensor)
			//publishTo(sensor)
		}(i)
	}
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
		//chn := make(chan DataEvent, len(eb.sensorsRequest))
		//ch = append(ch, chn)

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
		//eb.Publish(newSensor)

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

	// make channel
	AddDBBot(newBot)
	chn := make(chan DataEvent, len(eb.sensorsRequest))
	ch = append(ch, chn)
	eb.Subscribe(newBot)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newBot)
}

func killSensor(w http.ResponseWriter, r *http.Request) {

	//var id = "gni5otQjDyhUWmvBE8EWS4"

	fmt.Println("-- STO IN KILL SENSOR ")
	var id string
	var check bool

	json.NewDecoder(r.Body).Decode(&id)
	fmt.Println("id sensor: ", id)
	//var err bool
	check, _ = removeSensor(id)
	if check == false {
		fmt.Println("--- Error: sensor was not killed")
	} else {

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(id)

	}

}

func checkResilience() {

	resilience, err := GetResilienceEntries()
	if err != nil {
		panic(err)
	}

	requestSlice,err1 := GetRequestEntries()
	if err1 != nil {
		panic(err)
	}

	//once i got the system's state before ccrash i can release lock for main to gon on and listen and serve new requests
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

				if resilienceItem.Message == sensor.Message && strings.Contains(resilienceItem.Id, sensor.Id ) {

					requestResilienceEntries = append(requestResilienceEntries, resilienceItem)
				}
			}

			//retransmit the request to every entry
			for _, resilienceItem := range requestResilienceEntries {

				myBot := findBotbyId(strings.ReplaceAll(resilienceItem.Id,myRequestItem.Id,""))

				if myBot.Id == ""  {
					fmt.Println("NO BOT ASSOCIATED WITH THIS RESILIENCE ENTRY : SOMETHING WRONG \n")
					fmt.Println(strings.ReplaceAll(resilienceItem.Id,myRequestItem.Id,"") + "\n")

				}else if myBot.Id != ""{

					wg.Add(1)

					go publishImplementation(myBot, sensor, &wg)
				}

			}

			//awaits for all subroutines to end with an ack
			wg.Wait()

			removePubRequest(sensor.Id, sensor.Message)

			mainWg.Done()

		}(requestItem)

	}

	mainWg.Wait()
}


func findBotbyId(id string) Bot {

	for _,bot := range bots {

		if bot.Id == id {

			return bot
		}
	}
	var emptyBot Bot

	return emptyBot
}


//unsubscribes bot with a given Id from current topic
func unsubscribeBot(w http.ResponseWriter, r *http.Request) {

	/* Versione roberto, TODO TESTARE
	var newBot Bot
	json.NewDecoder(r.Body).Decode(&newBot)

	// chek errore!
	Unsubscribe(newBot)

	w.Header().Set("Content-Type", "application/json")
	// send back some ack
	json.NewEncoder(w).Encode(newBot)
	*/

	/*var id string
	var check bool

	json.NewDecoder(r.Body).Decode(&id)
	fmt.Println("id unsub: ", id)

	var newBot Bot
	newBot, _ = GetDBBot(id)
	if newBot.Id != id {
		fmt.Println("--- Error: wrong bot")
	}

	check, _ = removeTopic(id)
	if check == false {
		fmt.Println("--- Error: topic was not removed")
	} else {

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(newBot)

	}*/
}

//spawna un sensore randomico o un numero randomico di sensori?
/*
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
		sensorsRequest = append(sensorsRequest, newSensor)
		AddDBSensorRequest(newSensor)
		go func(sensor Sensor) {
			eb.Publish(newSensor)
			//publishTo(sensor)
		}(newSensor)
	}

	// make channel

	// what to return? 200 ? dunno
	//w.Header().Set("Content-Type", "application/json")
	//json.NewEncoder(w).Encode(newSensor)
}*/

/*
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
		spawned = append(spawned, true)
		AddDBBot(newBot)
		chn := make(chan DataEvent, len(sensorsRequest))
		ch = append(ch, chn)
		eb.Subscribe(newBot, chn)
	}

	// make channel

	// what to return? 200 ? dunno
	//w.Header().Set("Content-Type", "application/json")
	//json.NewEncoder(w).Encode(newBot)
}*/

/*
func unsubRandomBot(w http.ResponseWriter, r *http.Request) {

	fmt.Println("--- STO IN UNSUB RANDOM BOTS!!")

	var num = mux.Vars(r)["num"]
	totnum, err := strconv.Atoi(num)
	//json.NewDecoder(r.Body).Decode(&newBot)

	fmt.Println("--- totnum: ", totnum)

	if err != nil {
		// there was an error
		w.WriteHeader(400)
		w.Write([]byte("ID could not be converted to integer"))
		return
	}

	for i := 0; i < totnum; i++ {

		randomIndex := rand.Intn(len(sensorsRequest))
		pickedBot := bots[randomIndex]

		fmt.Println("--- pickedSensor id: ", pickedBot.Id)

		var check bool
		check, _ = removeTopic(pickedBot.Id)
		if check == false {
			fmt.Println("--- Error: not all bot were not unsubscribed")
		} else {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(pickedBot.Id)
		}

	}

}*/

/*
func killRandomBot(w http.ResponseWriter, r *http.Request) {

	fmt.Println("--- STO IN KILL RANDOM BOTS!!")

	var num = mux.Vars(r)["num"]
	totnum, err := strconv.Atoi(num)
	//json.NewDecoder(r.Body).Decode(&newBot)

	fmt.Println("--- totnum: ", totnum)

	if err != nil {
		// there was an error
		w.WriteHeader(400)
		w.Write([]byte("ID could not be converted to integer"))
		return
	}

	for i := 0; i < totnum; i++ {

		randomIndex := rand.Intn(len(sensorsRequest))
		pickedSensor := sensorsRequest[randomIndex]

		fmt.Println("--- pickedBot id: ", pickedSensor.Id)

		var check bool
		check, _ = removeBot(pickedSensor.Id)
		if check == false {
			fmt.Println("--- Error: bot was not killed")
		} else {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(pickedSensor.Id)
		}

	}

}

func killRandomSensor(w http.ResponseWriter, r *http.Request) {

	fmt.Println("--- STO IN KILL RANDOM SENSOR!!")

	var num = mux.Vars(r)["num"]
	totnum, err := strconv.Atoi(num)
	//json.NewDecoder(r.Body).Decode(&newBot)

	fmt.Println("--- totnum: ", totnum)

	if err != nil {
		// there was an error
		w.WriteHeader(400)
		w.Write([]byte("ID could not be converted to integer"))
		return
	}

	for i := 0; i < totnum; i++ {

		randomIndex := rand.Intn(len(sensorsRequest))
		pickedSensor := sensorsRequest[randomIndex]

		fmt.Println("--- pickedSensor id: ", pickedSensor.Id)

		var check bool
		check, _ = removeSensor(pickedSensor.Id)
		if check == false {
			fmt.Println("--- Error: sensor was not killed")
		} else {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(pickedSensor.Id)
		}

	}

}


*/

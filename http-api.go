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
}

// Sensor
type Sensor struct {
	Id            string `json:"id"`
	CurrentSector string `json:"current_sector"`
	Type          string `json:"type"`
	Message       string `json:"msg"`
	Pbrtx         bool   `json:"pbrtx"`
}

//resilience entry
type resilienceEntry struct {
	Id      string `json:"id"`
	Message string `json:"message"`
}

var bots []Bot
var sensorsRequest []Sensor
var spawned []bool
var warehouses []string
var topics []string
var ch []chan DataEvent
var contextLock = false
var results []Result
var dynamoDBSession *dynamodb.DynamoDB = nil

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

	/*
		fmt.Println("ECCHIME\n")
		var endlessWait sync.WaitGroup
		endlessWait.Add(1)
		go func() {

			for {

				//time.Sleep(5 * time.Second)
				if len(ch) > 0 {
					//fmt.Println(len(ch))

					for i := range ch {

						c := ch[i]

						if spawned[i] {

							fmt.Println(spawned[i])

							go func(c chan DataEvent, i int) {

								fmt.Println("Spawned routine for bot : " + bots[i].Id + "\n")
								spawned[i] = false
								fmt.Println(spawned[i])

								for {
									select {
									case d := <-c:

										//get the botId associated with this channel
										if botId, found := eb.botChannelIdMap[c]; found {
											// si puo simulare il crash dell'invio di un ack con un if qui prima di ch <- botId
											d.ResponseChannel <- botId
										}

										//TODO lock su results ? 3 commentata per evitare problemi se non sincronizzata
										/*
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

							}(c, i)
						}
					}

				}
			}

		}()*/

	fmt.Println("Chissa che gli prende \n")
	checkResilience()

	//checkDynamoSensorsCache()

	initWarehouseSpaces()
	initTopics()

	router.HandleFunc("/results", getResults).Methods("GET")

	router.HandleFunc("/status", heartBeatMonitoring).Methods("GET")
	router.HandleFunc("/switch", switchContext).Methods("POST")

	// TODO KILL BOT ROUTE
	router.HandleFunc("/unsubscribeBot", unsubscribeBot).Methods("POST")
	//router.HandleFunc("/unsubscribeRandomBot/{num}", unsubRandomBot).Methods("POST")
	router.HandleFunc("/killSingleBot", killBot).Methods("POST")
	//router.HandleFunc("/killRandomBot/{num}", killRandomBot).Methods("POST")
	router.HandleFunc("/bot", spawnBot).Methods("POST")
	//router.HandleFunc("/bot/{num}", spawnBotRand).Methods("POST")
	router.HandleFunc("/bot", listAllBots).Methods("GET")

	// TODO KILL BOT SENSOR
	//router.HandleFunc("/killRandomSensor/{num}", killRandomSensor).Methods("POST")
	router.HandleFunc("/killSingleSensor", killSensor).Methods("POST")
	router.HandleFunc("/sensor", spawnSensor).Methods("POST")
	//router.HandleFunc("/sensor/{num}", spawnSensorRand).Methods("POST")
	router.HandleFunc("/sensor", listAllSensors).Methods("GET")

	// linea standard per mettere in ascolto l'app. TODO controllo d'errore
	go func() {
		log.Fatal(http.ListenAndServe(":5000", router))
	}()

	//cicla sulla lista di richieste di tipo sensorRequests
	for {
		//servi sensor request[0]

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

func checkResilience() {

	res, err := GetResilienceEntries()
	if err != nil {
		panic(err)
	}

	fmt.Println("RESILIENCE CHECK SIZE IS : \n")
	fmt.Println(len(res))

	var wg sync.WaitGroup

	//for every bot there is a subroutine which sends the message to it and awaits for its ack
	for _, resilienceItem := range res {

		//fmt.Println("IO QUI  CI ENTRO : \n")
		ch := eb.botIdChannelMap[resilienceItem.Id]
		wg.Add(1)

		go func(ch chan DataEvent, message string, wg *sync.WaitGroup) {

			fmt.Println("IO QUI CI ENTRO 2 : \n")
			myChan := make(chan string)
			var data = DataEvent{Data: resilienceItem.Message, ResponseChannel: myChan}

			ch <- data
			//subroutine awaits for the ack from the bot

		L:
			for {

				select {

				case response := <-myChan:
					removeResilienceEntry(response, message)
					fmt.Println("Ack received from bot !")
					break L

				case <-time.After(10 * time.Second):

					close(myChan)
					myChan := make(chan string)
					data.ResponseChannel = myChan
					ch <- data
				}

			}

			wg.Done()

		}(ch, resilienceItem.Message, &wg)

	}

	//wait all subroutines have received their acks
	wg.Wait()
	fmt.Println("QUI CI ARRIVO E TERMINO !\n")

}

// retrieve sensor state from DB and initializes any sensor to spawn data (if any sensor is found)
func checkDynamoSensorsCache() {
	res, err := GetDBSensors()
	if err != nil {
		panic(err)
	}
	for _, i := range res {
		sensorsRequest = append(sensorsRequest, i)
		go func(sensor Sensor) {
			eb.Publish(sensor)
			//publishTo(sensor)
		}(i)
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
		spawned = append(spawned, true)
		chn := make(chan DataEvent, len(sensorsRequest))
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
		AddDBSensor(newSensor)
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

//spawns a new sensor with given values
func spawnSensor(w http.ResponseWriter, r *http.Request) {
	var newSensor Sensor

	//Genero un numero casuale tra 1 e 10 e se x>7 allora rispondo
	if rand.Intn(10) > 7 {
		json.NewDecoder(r.Body).Decode(&newSensor)

		//check if sensor already in system
		if newSensor.Id != "" {
			fmt.Println("New message from a sensor alredy in the system with id : " + newSensor.Id)
		} else {
			newSensor.Id = shortuuid.New()
			//sensorsRequest = append(sensorsRequest, newSensor)
			//AddDBSensor(newSensor)
		}

		var msg = newSensor.Message
		var ack = "Ack on message : " + msg + " on sensorn :" + newSensor.Id

		//check if message is a new message or a retransmission
		if newSensor.Pbrtx {

			fmt.Println("FACCIO FINTA DI ESEGUIRE IL SERVIZIO !")
		} else if !newSensor.Pbrtx {

			fmt.Println("STO ESEGUENDO IL SERVIZIO !")
			sensorsRequest = append(sensorsRequest, newSensor)
			//eb.Publish(newSensor)
			//TODO fillare la tabella dei sensori con tutti i campi della struct sensorsRequest
		}

		newSensor.Message = ack
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(newSensor)
	} else {
		time.Sleep(500000000 * time.Second)
		fmt.Println("NON RISPONDO!")
	}

}

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

//spawns a new bot with given values
func spawnBot(w http.ResponseWriter, r *http.Request) {
	var newBot Bot
	json.NewDecoder(r.Body).Decode(&newBot)
	newBot.Id = shortuuid.New()
	bots = append(bots, newBot)
	spawned = append(spawned, true)

	// make channel
	AddDBBot(newBot)
	chn := make(chan DataEvent, len(sensorsRequest))
	ch = append(ch, chn)
	eb.Subscribe(newBot, chn)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newBot)
}

//unsubscribes bot with a given Id from current topic
func unsubscribeBot(w http.ResponseWriter, r *http.Request) {

	var id string
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

	}

	//fmt.Printf("----len", len(eb.subscribersCtx))
	/*
		for i := 0; i < len(eb.subscribers); i++ {
			//fmt.Printf("value of a: %d\n", a)
			subscribers := eb.subscribers
			print(&subscribers)
		}

		print(eb.subscribersCtx)

	*/

}

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

}*/

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

//send bots list as response
func listAllBots(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// Fillo la response e mando la lista di bots
	json.NewEncoder(w).Encode(bots)
}

//send sensorsRequest list as response
func listAllSensors(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// Fillo la response e mando la lista di bots
	json.NewEncoder(w).Encode(sensorsRequest)
}

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

type key struct {
	Topic  string
	Sector string
}

type DataEvent struct {
	Data            interface{} // --> can be any value
	Topic           string
	ResponseChannel chan string
}

// BotSlice is a slice of Bot
type BotSlice []Bot

// Broker stores the information about subscribers interested for // a particular topic
type Broker struct {
	subscribersCtx map[key]BotSlice
	subscribers    map[string]BotSlice
	rm             sync.RWMutex // mutex protect broker against concurrent access from read and write

	sensorsRequest []Sensor
	lockQueue      sync.RWMutex
}

type subResponse struct {
	BotId   string `json:"id"`
	Message string `json:"message"`
}

// TODO Unsubscribe method!
func Unsubscribe(bot Bot) {
	// TODO TESTARE
	if contextLock == true {
		var internalKey = key{
			Topic:  bot.Topic,
			Sector: bot.CurrentSector,
		}
		// trovo l'indice nella slice
		for k, v := range eb.subscribersCtx[internalKey] {
			if bot == v {
				// lo rimuovo
				if prev, found := eb.subscribersCtx[internalKey]; found {
					eb.subscribersCtx[internalKey] = append(prev[:k], prev[k+1:]...)
				}
			}
		}
	} else {
		// trovo l'indice nella slice
		for k, v := range eb.subscribers[bot.Topic] {
			if bot == v {
				// lo rimuovo
				if prev, found := eb.subscribers[bot.Topic]; found {
					eb.subscribers[bot.Topic] = append(prev[:k], prev[k+1:]...)
				}
			}
		}
	}
}

func (eb *Broker) Subscribe(bot Bot) {

	eb.rm.Lock()
	if contextLock == true {
		// Context-Aware --> same work as without context but this time we need to search for a couple <Topic, Sector>
		var internalKey = key{
			Topic:  bot.Topic,
			Sector: bot.CurrentSector,
		}

		if prev, found := eb.subscribersCtx[internalKey]; found {
			eb.subscribersCtx[internalKey] = append(prev, bot)
		} else {
			eb.subscribersCtx[internalKey] = append([]Bot{}, bot)
		}

	} else {
		// Without context
		if prev, found := eb.subscribers[bot.Topic]; found {
			eb.subscribers[bot.Topic] = append(prev, bot)
		} else {
			eb.subscribers[bot.Topic] = append([]Bot{}, bot)
		}

	}

	eb.rm.Unlock()
}

func (eb *Broker) Publish(sensor Sensor) {

	localSensor := sensor
	eb.rm.RLock()

	if contextLock == true {
		var internalKey = key{
			Topic:  localSensor.Type,
			Sector: localSensor.CurrentSector,
		}
		if bots, found := eb.subscribersCtx[internalKey]; found {
			fmt.Println("Il lock funziona bene! \n")
			// this is done because the slices refer to same array even though they are passed by value
			// thus we are creating a new slice with our elements thus preserve locking correctly.
			myBots := append(BotSlice{}, bots...)
			eb.rm.RUnlock()

			//main subroutine spaws a subroutine for every bot who needs to be notified and awaits
			//for every subroutine to receive its own ack

			writeBotIdsAndMessage(myBots, localSensor)

			fmt.Println("RESILIENCE TABLE HAS BEEN FILLED FOR SENSOR : ! " + localSensor.Id + "\n")
			var wg sync.WaitGroup
			//for every bot there is a subroutine which sends the message to the bot and awaits for its ack
			for _, bot := range myBots {

				myBot := bot
				wg.Add(1)

				go publishImplementation(myBot, localSensor, &wg)

			}
			//wait all subroutines have received their acks
			wg.Wait()

			removePubRequest(localSensor.Id, localSensor.Message)
		} else {
			eb.rm.RUnlock()
			removePubRequest(localSensor.Id, localSensor.Message)
		}

	} else {

		if bots, found := eb.subscribers[localSensor.Type]; found {
			fmt.Println("Il lock funziona bene! \n")
			// this is done because the slices refer to same array even though they are passed by value
			// thus we are creating a new slice with our elements thus preserve locking correctly.
			myBots := append(BotSlice{}, bots...)
			eb.rm.RUnlock()

			//main subroutine spaws a subroutine for every bot who needs to be notified and awaits
			//for every subroutine to receive its own ack

			var wg sync.WaitGroup

			writeBotIdsAndMessage(myBots, localSensor)

			fmt.Println("RESILIENCE TABLE HAS BEEN FILLED FOR SENSOR : ! " + localSensor.Id + "\n")

			//for every bot there is a subroutine which sends the message to the bot and awaits for its ack
			for _, bot := range myBots {

				myBot := bot
				wg.Add(1)

				go publishImplementation(myBot, localSensor, &wg)

			}

			//wait all subroutines have received their acks
			wg.Wait()

			removePubRequest(localSensor.Id, localSensor.Message)

		} else {

			eb.rm.RUnlock()
			removePubRequest(localSensor.Id, localSensor.Message)

		}

	}

}

//retransmits a single message to a single bot until receives an ack from it (at least one semantic)
func publishImplementation(bot Bot, sensor Sensor, wg *sync.WaitGroup) {

	//subroutine awaits for the ack from the bot

	myNewBot := bot
	mySensor := sensor
	myMessage := mySensor.Message


	//blocking call : go function awaits for response to its http request
	response := newRequest(myNewBot, myMessage, mySensor)

	var dataReceived subResponse
	err := json.NewDecoder(response.Body).Decode(&dataReceived)


	if err == nil {

		// scenario in which bot responded with ack
		fmt.Println("Ack received successfully from bot :  " + dataReceived.BotId + "\n" + "with\n" +
			"Message:" + dataReceived.Message + "\n")

		removeResilienceEntry(dataReceived.BotId, dataReceived.Message, mySensor.Id)


	} else if err,ok := err.(net.Error); ok && err.Timeout() {

		fmt.Println("HOOKIN FOR BOOTY GOT TIMEOUT !!!")

		for {

			newResponse := newRequest(myNewBot, myMessage, mySensor)
			newErr := json.NewDecoder(newResponse.Body).Decode(&dataReceived)

			// got the right ack message so i cans top retransmitting
			if newErr == nil && dataReceived.BotId == myNewBot.Id && dataReceived.Message == myMessage {
				fmt.Println("HOOKIN FOR BOOTY GOT RIGHT ACK MESSAGE, GONNA STOP RETRANSMISSION !!!")
				break

			}else if newErr , ok := newErr.(net.Error); ok && newErr.Timeout(){
				fmt.Println("GOT TIMEOUTED AGAIN SO I WILL RETRY ONE TIME MORE !!!")
				continue

			}else{
				fmt.Println("GOT BOT ERROR NOT RESPONDING SO I WILL RETRY LATER ON !!!")
				time.Sleep(20*time.Second)
				continue
			}
		}

	}else {
		//got connection error
		fmt.Println(err)

	}


	wg.Done()
}

// function which generates a new http request to notify  bot with message
func newRequest(bot Bot, message string, sensor Sensor) *http.Response {

	request, err := json.Marshal(map[string]string{
		"msg":       message,
		"botId":     bot.Id,
		"bot_cs":    bot.CurrentSector,
		"sensor":    sensor.Id,
		"sensor_cs": sensor.CurrentSector,
	})

	if err != nil {
		fmt.Println(err)
		return nil
	}

	resp, err := http.Post("http://"+bot.IpAddress+":5001/", "application/json", bytes.NewBuffer(request))

	if err != nil {
		fmt.Println(err)
		return nil
	}
	return resp
}

// init broker
var eb = &Broker{
	subscribers:    map[string]BotSlice{},
	subscribersCtx: map[key]BotSlice{},
	sensorsRequest: []Sensor{},
}

/*  MAYBE FUTURE IMPLEMENTATION?
func swapCtxToStandard() {
	for _, i := range eb.subscribersCtx {

	}
}

func swapStandardToNormal() {

}*/

//function that allow sensor to publish data in an infinite loop
/*func publishTo(sensor Sensor) {

	for {
		eb.Publish(sensor, strconv.FormatFloat(rand.Float64(), 'E', 1, 64))
		time.Sleep(time.Duration(rand.Intn(50)) * time.Second)


	}
}*/

/*func printDataEvent(ch string, data DataEvent) {
	fmt.Printf("Channel: %s; Topic: %s; DataEvent: %v\n", ch, data.Topic, data.Data)
}*/

/*func main() {
	ch1 := make(chan DataEvent)
	ch2 := make(chan DataEvent)
	ch3 := make(chan DataEvent)
	eb.Subscribe("topic1", ch1)
	eb.Subscribe("topic2", ch2)
	eb.Subscribe("topic2", ch3)
	go publishTo("topic1", "Hi topic 1")
	go publishTo("topic2", "Welcome to topic 2")
	for {
		select { // select will get data from the quickest channel
		case d := <-ch1:
			go printDataEvent("ch1", d)
		case d := <-ch2:
			go printDataEvent("ch2", d)
		case d := <-ch3:
			go printDataEvent("ch3", d)
		}
	}
}*/

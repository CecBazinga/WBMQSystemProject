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

func (eb *Broker) Unsubscribe(myBot Bot) {
	bot := myBot
	eb.rm.Lock()
	if contextLock == true {
		var internalKey = key{
			Topic:  bot.Topic,
			Sector: bot.CurrentSector,
		}
		// Trovo indice del bot
		for k, theBot := range eb.subscribersCtx[internalKey] {
			if theBot.Id == bot.Id {
				// lo rimuovo
				eb.subscribersCtx[internalKey] = append(eb.subscribersCtx[internalKey][:k], eb.subscribersCtx[internalKey][k+1:]...)
				break
			}
		}

	} else {
		// Trovo indice del bot
		for k, theBot := range eb.subscribers[bot.Topic] {
			if theBot.Id == bot.Id {
				// lo rimuovo
				eb.subscribers[bot.Topic] = append(eb.subscribers[bot.Topic][:k], eb.subscribers[bot.Topic][k+1:]...)
				break
			}
		}
	}
	eb.rm.Unlock()

	err := removeBot(bot.Id)
	if err != nil {
		panic("Got error in removing bot")
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

		if notThebots, found := eb.subscribersCtx[internalKey]; found {

			// this is done because the slices refer to same array even though they are passed by value
			// thus we are creating a new slice with our elements thus preserve locking correctly.
			myBots := append(BotSlice{}, notThebots...)
			eb.rm.RUnlock()

			//main subroutine spaws a subroutine for every bot who needs to be notified and awaits
			//for every subroutine to receive its own ack

			writeBotIdsAndMessage(myBots, localSensor)

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

		if notThebots, found := eb.subscribers[localSensor.Type]; found {

			// this is done because the slices refer to same array even though they are passed by value
			// thus we are creating a new slice with our elements thus preserve locking correctly.
			myBots := append(BotSlice{}, notThebots...)
			eb.rm.RUnlock()

			//main subroutine spaws a subroutine for every bot who needs to be notified and awaits
			//for every subroutine to receive its own ack

			var wg sync.WaitGroup

			writeBotIdsAndMessage(myBots, localSensor)

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
		removeResilienceEntry(dataReceived.BotId, dataReceived.Message, mySensor.Id)

	} else if err, ok := err.(net.Error); ok && err.Timeout() {

		for {

			newResponse := newRequest(myNewBot, myMessage, mySensor)
			newErr := json.NewDecoder(newResponse.Body).Decode(&dataReceived)

			// got the right ack message so i cans top retransmitting
			if newErr == nil && dataReceived.BotId == myNewBot.Id && dataReceived.Message == myMessage {

				// qua non serve fare la removeresilianceentry?
				break

			} else if newErr, ok := newErr.(net.Error); ok && newErr.Timeout() {
				continue

			} else {
				time.Sleep(20 * time.Second)
				continue
			}
		}

	} else {
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
		"topic":     bot.Topic,
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

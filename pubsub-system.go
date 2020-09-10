package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

/*

	-----------------------------------DataChannelSlice---------------------------------------
	|
	|
	|				-------------------DataChannel0------------------------------------
	|				|
	|				|   *---DataEvent0----*   *---DataEvent1----*
	|				|	|  Data: "msg"	  |	  |  Data: "msg"	|     ...    ...
	|				|	|  Topic: "topic" |   |  Topic: "topic" |
	|				|	*-----------------*   *-----------------*
	|				|
	|				-------------------------------------------------------------------
	|
	|				--------------------DataChannel1-----------------------------------
	|				|
	|				|   *---DataEvent0----*   *---DataEvent1----*
	|				|	|  Data: "msg"	  |	  |  Data: "msg"	|     ...    ...
	|				|	|  Topic: "topic" |   |  Topic: "topic" |
	|				|	*-----------------*   *-----------------*
	|				|
	|				--------------------------------------------------------------------
	|
	|
	|							...
	|
	|							...
	|
	-----------------------------------------------------------------------------------------

*/

var IDMapToChannel []string
var SectorMapping []string

type key struct {
	Topic  string
	Sector string
}

type DataEvent struct {
	Data  interface{} // --> can be any value
	Topic string
}

// DataChannel is a channel which can accept a DataEvent
type DataChannel chan DataEvent

// DataChannelSlice is a slice of DataChannels
type DataChannelSlice []DataChannel

// Broker stores the information about subscribers interested for // a particular topic
type Broker struct {
	subscribersCtx map[key]DataChannelSlice
	subscribers    map[string]DataChannelSlice
	rm             sync.RWMutex // mutex protect broker against concurrent access from read and write
}

// TODO Unsubscribe method!

func (eb *Broker) Subscribe(bot Bot, ch DataChannel) {
	IDMapToChannel = append(IDMapToChannel, bot.Id)
	SectorMapping = append(SectorMapping, bot.CurrentSector)
	eb.rm.Lock()
	if contextLock == true {
		// Context-Aware --> same work as without context but this time we need to search for a couple <Topic, Sector>
		var internalKey = key{
			Topic:  bot.Topic,
			Sector: bot.CurrentSector,
		}
		if prev, found := eb.subscribersCtx[internalKey]; found {
			eb.subscribersCtx[internalKey] = append(prev, ch)
		} else {
			eb.subscribersCtx[internalKey] = append([]DataChannel{}, ch)
		}
	} else {
		// Without context
		if prev, found := eb.subscribers[bot.Topic]; found {
			eb.subscribers[bot.Topic] = append(prev, ch)
		} else {
			eb.subscribers[bot.Topic] = append([]DataChannel{}, ch)
		}
	}
	eb.rm.Unlock()
}

func (eb *Broker) Publish(sensor Sensor, data interface{}) {
	eb.rm.RLock()
	if contextLock == true {
		var internalKey = key{
			Topic:  sensor.Type,
			Sector: sensor.CurrentSector,
		}
		if chans, found := eb.subscribersCtx[internalKey]; found {
			// this is done because the slices refer to same array even though they are passed by value
			// thus we are creating a new slice with our elements thus preserve locking correctly.
			channels := append(DataChannelSlice{}, chans...)
			go func(data DataEvent, dataChannelSlices DataChannelSlice) {
				for _, ch := range dataChannelSlices {
					ch <- data
				}
				//TODO ci metto un array di boolean uno per ogni channel dei bot e faccio mettere a true il valore se ha ricevuto il messaggio
				//ritrasmetto i false

			}(DataEvent{Data: data, Topic: sensor.Type}, channels)
		}
	} else {
		if chans, found := eb.subscribers[sensor.Type]; found {
			// this is done because the slices refer to same array even though they are passed by value
			// thus we are creating a new slice with our elements thus preserve locking correctly.
			channels := append(DataChannelSlice{}, chans...)
			go func(data DataEvent, dataChannelSlices DataChannelSlice) {
				for _, ch := range dataChannelSlices {
					ch <- data
				}
			}(DataEvent{Data: data, Topic: sensor.Type}, channels)
		}
	}
	eb.rm.RUnlock()
}

// init broker
var eb = &Broker{
	subscribers:    map[string]DataChannelSlice{},
	subscribersCtx: map[key]DataChannelSlice{},
}

/*  MAYBE FUTURE IMPLEMENTATION?
func swapCtxToStandard() {
	for _, i := range eb.subscribersCtx {

	}
}

func swapStandardToNormal() {

}*/

//function that allow sensor to publish data in an infinite loop
func publishTo(sensor Sensor) {
	for {

		eb.Publish(sensor, strconv.FormatFloat(rand.Float64(), 'E', 1, 64))
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	}
}

func printDataEvent(ch string, data DataEvent) {
	fmt.Printf("Channel: %s; Topic: %s; DataEvent: %v\n", ch, data.Topic, data.Data)
}

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

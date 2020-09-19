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
	Data            interface{} // --> can be any value
	Topic           string
	ResponseChannel chan string
}

// DataChannel is a channel which can accept a DataEvent
type DataChannel chan DataEvent

// DataChannelSlice is a slice of DataChannels
type DataChannelSlice []DataChannel

// StringSlice is a slice of strings
type StringSlice []string

// Broker stores the information about subscribers interested for // a particular topic
type Broker struct {
	subscribersCtx map[key]DataChannelSlice
	subscribers    map[string]DataChannelSlice

	botIdChannelMap  map[string]DataChannel
	botTopicCtxIdMap map[key]StringSlice
	botTopicIdMap    map[string]StringSlice
	botChannelIdMap  map[chan DataEvent]string

	rm sync.RWMutex // mutex protect broker against concurrent access from read and write
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

		//create mapping botTopicCtx-> botId
		if prev, found := eb.botTopicCtxIdMap[internalKey]; found {
			eb.botTopicCtxIdMap[internalKey] = append(prev, bot.Id)
		} else {
			eb.botTopicCtxIdMap[internalKey] = append([]string{}, bot.Id)
		}

	} else {
		// Without context
		if prev, found := eb.subscribers[bot.Topic]; found {
			eb.subscribers[bot.Topic] = append(prev, ch)
		} else {
			eb.subscribers[bot.Topic] = append([]DataChannel{}, ch)
		}

		//create mapping botTopic -> botId
		if prev, found := eb.botTopicIdMap[bot.Topic]; found {
			eb.botTopicIdMap[bot.Topic] = append(prev, bot.Id)
		} else {
			eb.botTopicIdMap[bot.Topic] = append([]string{}, bot.Id)
		}
	}

	//create mapping botId -> botChannel
	eb.botIdChannelMap[bot.Id] = ch

	//create mapping botChannel -> botId
	eb.botChannelIdMap[ch] = bot.Id

	eb.rm.Unlock()
}

func (eb *Broker) Publish(sensor Sensor, data interface{}) {

	//TODO come facciamo a lascaire l'event broker viable per altri sensori che vogliono pubblicare senza killare la main subroutine? magari lo circoscriviamo solo
	//TODO alla creazione delle subroutine cosi locka gli array in uso e quando non servono piu lo slockiamo : unlock-->wait main subroutine e dopo l'unlock può
	//TODO servire altri sensori 1

	eb.rm.RLock()

	message := data.(string)
	fmt.Println("MESSAGE IS : " + message + "\n")

	if contextLock == true {
		var internalKey = key{
			Topic:  sensor.Type,
			Sector: sensor.CurrentSector,
		}
		if chans, found := eb.subscribersCtx[internalKey]; found {
			// this is done because the slices refer to same array even though they are passed by value
			// thus we are creating a new slice with our elements thus preserve locking correctly.
			channels := append(DataChannelSlice{}, chans...)

			var wgMainSubroutine sync.WaitGroup
			//main subroutine which spaws a subroutine for every bot who needs to be notificated and awaits
			//for every subroutine to receive its pwn ack
			go func(data DataEvent, dataChannelSlices DataChannelSlice, message string) {

				wgMainSubroutine.Add(1)

				var wg sync.WaitGroup
				var botIdsArray []string

				if botIds, found := eb.botTopicCtxIdMap[internalKey]; found {

					botIdsArray = append(StringSlice{}, botIds...)
					writeBotIdsAndMessage(botIdsArray, message)

					fmt.Println("RESILIENCE TABLE HAS BEEN FILLED ! \n")
					time.Sleep(10 * time.Second)

				} else {
					//TODO forwardare al frontend
					fmt.Printf("No bots are subscribed to this sensor type : %v with ctx : %v \n",
						sensor.Type, sensor.CurrentSector)
				}

				//for every bot there is a subroutine which sends the message to it and awaits for its ack
				for _, ch := range dataChannelSlices {

					wg.Add(1)

					go func(ch chan DataEvent, message string, data DataEvent, wg *sync.WaitGroup) {

						myChan := make(chan string)
						data.ResponseChannel = myChan

						ch <- data
						//subroutine awaits for the ack from the bot
					L:
						for {

							select {

							case response := <-myChan:
								fmt.Println("BOT ID IS : " + response + "\n")
								removeResilienceEntry(response, message)
								break L

							case <-time.After(5 * time.Second):

								close(myChan)
								myChan := make(chan string)
								data.ResponseChannel = myChan
								ch <- data
							}

						}

						fmt.Println("Ack received from bot !")
						wg.Done()

					}(ch, message, DataEvent{Data: data, Topic: sensor.Type}, &wg)

				}

				//TODO magari unlock qui dell' eb va piu che bene 1
				//wait all subroutines have received their acks
				wg.Wait()

				wgMainSubroutine.Done()

			}(DataEvent{Data: data, Topic: sensor.Type}, channels, message)

			wgMainSubroutine.Wait()
		}

	} else {

		if chans, found := eb.subscribers[sensor.Type]; found {
			// this is done because the slices refer to same array even though they are passed by value
			// thus we are creating a new slice with our elements thus preserve locking correctly.
			channels := append(DataChannelSlice{}, chans...)

			var wgMainSubroutine sync.WaitGroup

			//main subroutine which spaws a subroutine for every bot who needs to be notificated and awaits
			//for every subroutine to receive its pwn ack
			go func(data DataEvent, dataChannelSlices DataChannelSlice, message string) {

				wgMainSubroutine.Add(1)

				var wg sync.WaitGroup
				var botIdsArray []string

				if botIds, found := eb.botTopicIdMap[sensor.Type]; found {

					botIdsArray = append(StringSlice{}, botIds...)
					writeBotIdsAndMessage(botIdsArray, message)

				} else {
					fmt.Println("No bots are subscribed to this sensor type : " + sensor.Type + "\n")
				}

				//for every bot there is a subroutine which sends the message to it and awaits for its ack
				for _, ch := range dataChannelSlices {

					wg.Add(1)

					go func(ch chan DataEvent, message string, data DataEvent, wg *sync.WaitGroup) {

						myChan := make(chan string)
						data.ResponseChannel = myChan

						ch <- data
						//subroutine awaits for the ack from the bot
					L:
						for {

							select {

							case response := <-myChan:
								removeResilienceEntry(response, message)
								break L

							case <-time.After(10 * time.Second):

								close(myChan)
								myChan := make(chan string)
								data.ResponseChannel = myChan
								ch <- data
							}

						}

						fmt.Println("Ack received from bot !")

						wg.Done()

					}(ch, message, DataEvent{Data: data, Topic: sensor.Type}, &wg)

				}

				//TODO magari unlock qui dell' eb va piu che bene 1
				//wait all subroutines have received their acks
				wg.Wait()

				wgMainSubroutine.Done()

			}(DataEvent{Data: data, Topic: sensor.Type}, channels, message)

			wgMainSubroutine.Wait()

		}
	}
	eb.rm.RUnlock()
}

// init broker
var eb = &Broker{
	subscribers:      map[string]DataChannelSlice{},
	subscribersCtx:   map[key]DataChannelSlice{},
	botIdChannelMap:  map[string]DataChannel{},
	botTopicCtxIdMap: map[key]StringSlice{},
	botTopicIdMap:    map[string]StringSlice{},
	botChannelIdMap:  map[chan DataEvent]string{},
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
		//time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		time.Sleep(20 * time.Second)
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

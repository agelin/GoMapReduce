package mr

import (
	"encoding/json"
	"hash"
	"hash/adler32"
	"log"
	"net"
	"sync"
	"time"
)

// Map of reducer num to reducer data.
// Needs to be initialized and protected from concurrent access by mappers.
var allRData map[int]map[string][]string

// Mutex to protect allRData
var mutex sync.Mutex

// Channel on which "quit" message will arrive from launched sets of mappers
var mapDoneChan chan bool

// Channel on which "quit" message will arrive from launched sets of reducers
var redDoneChan chan bool

// Number of times "MapMSG" was received
var numMapData int

// Number of times "ReduceMSG" (command to start reducer) was received
var numRedData int

// Hash used to calculate hash of intermediate key
var h hash.Hash32

// Collects map output from different workers
var rDataMap map[string][]string

// Keeps track of which workers this workers still needs to collect map output data
var mapOutWorkers map[int]bool

func RunWorker(mr MapReduce, quitWorker chan bool) {

	// Initialize data structures
	allRData = make(map[int]map[string][]string)
	numMapData = 0
	numRedData = 0
	mapDoneChan = make(chan bool)
	redDoneChan = make(chan bool)
	rDataMap = make(map[string][]string) // accumulated data for reducers
	mapOutWorkers := make (map[int] bool)
	h = adler32.New()

	// Listen to incoming requests
	l, err := net.Listen("tcp", MyIP)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("W%d : Listening at IP %s\n", MyRank, MyIP)

	log.Printf("W%d : In UW State\n", MyRank)
	// Uninitialized worker
	// Block on an incoming connection from master
	// If the master is up, it will initiate a TCP connection
	// and close it immediately.
	c, err := l.Accept()
	log.Printf("W%d : Received connection from master, initialized\n", MyRank)
	if err != nil {
		log.Fatal(err)
	}
	if err := c.Close(); err != nil {
		log.Fatal(err)
	}

	/**************************************************************/
	/* 			Initialized worker now listening to				  */
	/*			master & other worker action messages  			  */
	/**************************************************************/

	for {

		log.Printf("W%d : In IW State\n", MyRank)

		// Initialized worker
		var c net.Conn
		var err error
		if c, err = l.Accept(); err != nil {
			log.Fatal(err)
		}

		go func(c net.Conn) {

			var mode string

			//DEBUG
			//			var msg ActionMessage
			//			str := DebugJSON(b)
			//			mdec := json.NewDecoder(strings.NewReader(str))
			//			if err := mdec.Decode(&msg); err != nil {
			//				log.Fatal(err)
			//			}

			var msg ActionMessage
			dec := json.NewDecoder(c)
			if err := dec.Decode(&msg); err != nil {
				log.Fatal(err)
			}

			mode = msg.Msg

			// DEBUG
			log.Printf("W%d : Received message - %d\n", MyRank, mode)

			switch mode {
			case MapMSG:
				numMapData = numMapData + 1
				log.Printf("W%d : Got %dth \"MapMSG\" \n", MyRank, numMapData)

				// Get Map Data to start map tasks

				// DEBUG
				//				var md MapData
				//				str := DebugJSON(b)
				//				dec := json.NewDecoder(strings.NewReader(str))
				//				if err := dec.Decode(&md); err != nil {
				//					log.Fatal(err)
				//				}

				var md MapData
				//				dec := json.NewDecoder(c)
				if err := dec.Decode(&md); err != nil {
					log.Fatal(err)
				}

				log.Printf("W%d : Collected Map data from master\n", MyRank)

				mchans := make(map[string]chan Pair) // List of mapper channels
				// Start map tasks with incoming data
				for k, v := range md.M {
					ch := make(chan Pair)
					mchans[k] = ch

					go func(k string, v string, ch chan Pair) {
						mr.Mapper(k, v, ch)
						close(ch)
					}(k, v, ch)
				}
				log.Printf("W%d : Launched %d map tasks\n", MyRank, len(mchans))

				// Put data into allRData
				go func() {
					ich := fanInChannel(mchans)
					for p := range ich {
						key := p.First
						val := p.Second
						h.Write([]byte(key))
						// Hash key to get worker rank to set as reducer
						redRank := int(h.Sum32())%(len(NodesMap)-1) + 1

						if redRank == 0 {
							log.Fatal("Calculated rank based on hash of key was 0 !")
						}

						h.Reset()
						// Put calculated rank and data into allRData
						mutex.Lock()
						m, ok := allRData[redRank]
						if !ok {
							m = make(map[string][]string)
						}
						vallist, ok := m[key]
						if !ok {
							vallist = make([]string, 0)
						}
						vallist = append(vallist, val)
						m[key] = vallist
						allRData[redRank] = m
						mutex.Unlock()
					}
					log.Printf("W%d - Done collecting data from all mappers !\n", MyRank)
					// we can detect when they're done
					mapDoneChan <- true
				}()

				log.Printf("W%d - Launched async method to collect all map data\n", MyRank)

				//log.Println("MapData :" +  md.m)
				//log.Println("MapData")

			case EndOfMapMSG:

				log.Printf("W%d : Got \"EndOfMapMSG\" \n", MyRank)

				// Wait for completion of all Map Tasks
				for i := 0; i < numMapData; i++ {
					<-mapDoneChan
				}

				numMapData = 0

				log.Printf("W%d : Completed all map tasks\n", MyRank)
				// Collect reader information and send to master

				// Collect data
				var m2r MapperToReducersInfo
				m2r.Mapper = MyRank
				m2r.Reducers = make([]int, len(allRData))
				i := 0
				for k, _ := range allRData {
					m2r.Reducers[i] = k
					i = i + 1
				}
				log.Printf("W%d : Collected set of reducers to send to master\n", MyRank)

				// Connect to master
				masterAddr := NodesMap[0]
				var mc net.Conn
				if mc, err = RetryDial("tcp", masterAddr, DialTimeOut*time.Second); err != nil {
					log.Fatal(err)
				}

				// Send master "ReduceWorkersMSG"
				var msg ActionMessage
				msg.Msg = ReduceWorkersMSG
				menc := json.NewEncoder(mc)
				if err = menc.Encode(&msg); err != nil {
					log.Fatal(err)
				}
				log.Printf("W%d : Send master the \"ReduceWorkersMSG\" message\n", MyRank)

				// Send reduce worker list to master
				enc := json.NewEncoder(mc)
				if err = enc.Encode(&m2r); err != nil {
					log.Fatal(err)
				}
				log.Printf("W%d : Send master the reducer list\n", MyRank)

				if err := mc.Close(); err != nil {
					log.Fatal(err)
				}

			case MapOutputDataMSG:
				log.Printf("W%d : Got \"MapOutputDataMSG\" \n", MyRank)

				var redd ReduceData
				if err = dec.Decode(&redd); err != nil {
					log.Fatal(err)
				}

				log.Printf("W%d : Got reducer input data from worker %d\n", MyRank, redd.Mapper)

				// accumulate data
				for k, v := range redd.M {
					mutex.Lock()	
					lst, ok := rDataMap[k]
					if !ok {
						lst = make([]string, 0)
					}
					lst = append(lst, v...)
					rDataMap[k] = lst
					mutex.Unlock()
				}
				log.Printf("W%d : Combined the data into rDataMap from worker %d\n", MyRank, redd.Mapper)

				mutex.Lock()	
				delete(mapOutWorkers, redd.Mapper)
				mutex.Unlock()
				
				log.Printf("M : Deleted mapper %d from set of mapOutWorkers\n", redd.Mapper)

				mutex.Lock()
				if len(mapOutWorkers) == 0 {

					log.Printf("W%d : Done collecting all reducer input data\n", MyRank)

					// Start reduce jobs
					rchans := make(map[string]chan Pair)
					for k, v := range rDataMap {
						ch := make(chan Pair)
						rchans[k] = ch

						go func(k string, v []string, ch chan Pair) {
							mr.Reducer(k, v, ch)
							close(ch)
						}(k, v, ch)
					}

					log.Printf("W%d : Launched all reducers\n", MyRank)

					go func() {
						ich := fanInChannel(rchans)
						var rd ReducedData
						rd.Reducer = MyRank
						rd.Data = make([]Pair, 0)
						for p := range ich {
							rd.Data = append(rd.Data, p)
						}
						log.Printf("W%d : all reducers done, collected output into ReduceData instance\n", MyRank)

						// Send data to master
						masterAddr := NodesMap[0]
						var mc net.Conn
						if mc, err = RetryDial("tcp", masterAddr, DialTimeOut*time.Second); err != nil {
							log.Fatal(err)
						}

						var msg ActionMessage
						msg.Msg = ReducedDataMSG
						menc := json.NewEncoder(mc)
						if err = menc.Encode(&msg); err != nil {
							log.Fatal(err)
						}

						log.Printf("W%d : sent \"ReducedDataMSG\" message to master\n", MyRank)

						// Send reduced data
						enc := json.NewEncoder(mc)
						if err = enc.Encode(&rd); err != nil {
							log.Fatal(err)
						}

						log.Printf("W%d : sent all reduced data to master\n", MyRank)

						redDoneChan <- true
					}()

					log.Printf("W%d : launched async routine to collect all reducer output and send to master\n", MyRank)
				}
				mutex.Unlock()

			case ReduceMSG:
				log.Printf("W%d : Got \"ReduceMSG\" \n", MyRank)

				// Get list of IWs to get data from
				numRedData = numRedData + 1
				var iwr IWRanks
				//				dec := json.NewDecoder(c)
				if err := dec.Decode(&iwr); err != nil {
					log.Fatal(err)
				}
				log.Printf("W%d : Got ranks of workers(%d) to get reduce data from\n", MyRank, len(iwr.Ranks))
				//log.Println("Reducer : Mapper ranks to get data from : " + iwr.ranks)
				//log.Println("Reducer : Mapper ranks to get data")

				// Initiate connection with all IWs, collect data & start reducers.

				for _, rank := range iwr.Ranks {
					log.Printf("W%d : Getting data from worker %d\n", MyRank, rank)
					var redd ReduceData // data from each IW
					if rank != MyRank {
						iwaddr := NodesMap[rank]
						var iwc net.Conn
						if iwc, err = RetryDial("tcp", iwaddr, DialTimeOut*time.Second); err != nil {
							log.Fatal(err)
						}

						var msg ActionMessage
						msg.Msg = IWDataMSG
						menc := json.NewEncoder(iwc)
						if err = menc.Encode(&msg); err != nil {
							log.Fatal(err)
						}
						log.Printf("W%d : Sent \"IWDataMSG\" message to worker %d\n", MyRank, rank)

						// Send rank
						var rd RequestData
						rd.Rank = MyRank
						enc := json.NewEncoder(iwc)
						if err = enc.Encode(&rd); err != nil {
							log.Fatal(err)
						}
						log.Printf("W%d : Sent own rank to worker to get reducer input data from worker %d\n", MyRank, rank)
						
						mutex.Lock()
						mapOutWorkers[rank] = true
						mutex.Unlock()

						// Expect data on same channel
						//						ddec := json.NewDecoder(iwc)
						//						if err = ddec.Decode(&redd); err != nil {
						//							log.Fatal(err)
						//						}
						//
						//						log.Printf("W%d : Got all reducer input data from worker %d\n", MyRank, rank)
						//
						//						if err := iwc.Close(); err != nil {
						//							log.Fatal(err)
						//						}
						
						
						
					} else { // If i have data, just copy it over
						mutex.Lock()
						redd.M = allRData[MyRank]
						mutex.Unlock()
						log.Printf("W%d : I had the data, just copied it over\n", MyRank)

					}

					// accumulate data
					//					for k, v := range redd.M {
					//						lst, ok := rdatamap[k]
					//						if !ok {
					//							lst = make([]string, 0)
					//						}
					//						lst = append(lst, v...)
					//						rdatamap[k] = lst
					//					}
					//					log.Printf("W%d : Combined the data into rdatamap from worker %d\n", MyRank, rank)
				}

				//				log.Printf("W%d : Done collecting all reducer input data\n", MyRank)
				//
				//				// Start reduce jobs
				//				rchans := make(map[string]chan Pair)
				//				for k, v := range rdatamap {
				//					ch := make(chan Pair)
				//					rchans[k] = ch
				//
				//					go func(k string, v []string, ch chan Pair) {
				//						mr.Reducer(k, v, ch)
				//						close(ch)
				//					}(k, v, ch)
				//				}
				//
				//				log.Printf("W%d : Launched all reducers\n", MyRank)
				//
				//				go func() {
				//					ich := fanInChannel(rchans)
				//					var rd ReducedData
				//					rd.Reducer = MyRank
				//					rd.Data = make([]Pair, 0)
				//					for p := range ich {
				//						rd.Data = append(rd.Data, p)
				//					}
				//					log.Printf("W%d : all reducers done, collected output into ReduceData instance\n", MyRank)
				//
				//					// Send data to master
				//					masterAddr := NodesMap[0]
				//					var mc net.Conn
				//					if mc, err = RetryDial("tcp", masterAddr, DialTimeOut*time.Second); err != nil {
				//						log.Fatal(err)
				//					}
				//
				//					var msg ActionMessage
				//					msg.Msg = ReducedDataMSG
				//					menc := json.NewEncoder(mc)
				//					if err = menc.Encode(&msg); err != nil {
				//						log.Fatal(err)
				//					}
				//
				//					log.Printf("W%d : sent \"ReducedDataMSG\" message to master\n", MyRank)
				//
				//					// Send reduced data
				//					enc := json.NewEncoder(mc)
				//					if err = enc.Encode(&rd); err != nil {
				//						log.Fatal(err)
				//					}
				//
				//					log.Printf("W%d : sent all reduced data to master\n", MyRank)
				//
				//					redDoneChan <- true
				//				}()
				//
				//				log.Printf("W%d : launched async routine to collect all reducer output and send to master\n", MyRank)

			case IWDataMSG:

				log.Printf("W%d : Got \"IWDataMSG\" \n", MyRank)

				// Read rank of IW
				var rd RequestData
				//				dec := json.NewDecoder(c)
				if err := dec.Decode(&rd); err != nil {
					log.Fatal(err)
				}
				log.Printf("W%d : Got request to send reducer input data to worker %d\n", MyRank, rd.Rank)

				// Send MapOutputDataMSG message to worker
				var rc net.Conn
				wip := NodesMap[rd.Rank]
				if rc, err = RetryDial("tcp", wip, DialTimeOut*time.Second); err != nil {
					log.Fatal(err)
				}
				var msg ActionMessage
				msg.Msg = MapOutputDataMSG
				menc := json.NewEncoder(rc)
				if err = menc.Encode(&msg); err != nil {
					log.Fatal(err)
				}
				log.Printf("W%d : Send MapOutputDataMSG message to worker %d\n", MyRank, rd.Rank)

				// Send data to IW
				reduceData := allRData[rd.Rank]
				var redd ReduceData
				redd.M = reduceData
				redd.Mapper = MyRank
				enc := json.NewEncoder(rc)
				if err := enc.Encode(&redd); err != nil {
					log.Fatal(err)
				}
				log.Printf("W%d : Send reducer data input (ReduceData instance) to worker %d\n", MyRank, rd.Rank)

			case EndLifeMSG:

				log.Printf("W%d : Got \"EndLifeMSG\" \n", MyRank)

				// Wait for all reduce jobs to finish
				for i := 0; i < numRedData; i++ {
					<-redDoneChan
				}
				log.Printf("W%d : All reducers are done! \n", MyRank)

				/**************************************************************/
				/*					End life of the worker	  				  */
				/**************************************************************/
				log.Printf("W%d : Stopping Worker\n", MyRank)
				quitWorker <- true

			default:
				log.Fatal("Not a supported message type... exiting!")
			}
			if err := c.Close(); err != nil {
				log.Fatal(err)
			}
		}(c)
	}

}

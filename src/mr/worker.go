package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash"
	"hash/adler32"
	"log"
	"net"
	"os"
	"sync"
)

const (
	IW = iota // Initialized worker
	UW = iota // Unintialized worker
	SW = iota // Stopped worker
)

// Map of reducer num to reducer data.
// Needs to be initialized and protected from concurrent access by mappers.
var allRData map[int]map[string][]string
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

func RunWorker(mr MapReduce) {

	// Initialize data structures
	allRData = make(map[int]map[string][]string)
	numMapData = 0
	numRedData = 0
	mapDoneChan = make(chan bool)
	redDoneChan = make(chan bool)
	h = adler32.New()

	state := UW // Initially all workers are uninitialized

	// Listen to incoming requests
	l, err := net.Listen("tcp", MyIP)
	if err != nil {
		log.Fatal(err)
	}

	for {

		switch state {

		case UW:
			// Uninitialized worker
			// Block on an incoming connection from master
			// If the master is up, it will initiate a TCP connection
			// and close it immediately.
			c, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}
			state = IW
			c.Close()

		case IW:
			// Initialized worker
			var c net.Conn
			var err error
			if c, err = l.Accept(); err != nil {
				log.Fatal(err)
			}

			b := bufio.NewReader(c)
			var mode byte
			if mode, err = b.ReadByte(); err != nil {
				log.Fatal(err)
			}

			// DEBUG
			fmt.Printf("W%d : Received message - %d\n", MyRank, mode)

			switch mode {
			case MapMSG:
				numMapData = numMapData + 1
				// Get Map Data to start map tasks
				var md MapData
				dec := json.NewDecoder(b)
				if err := dec.Decode(&md); err != nil {
					log.Fatal(err)
				}
				// Start map tasks with incoming data

				// List of mapper channels
				mchans := make(map[string]chan Pair)
				for k, v := range md.m {
					ch := make(chan Pair)
					mchans[k] = ch

					go func(k string, v string, ch chan Pair) {
						mr.Mapper(k, v, ch)
						close(ch)
					}(k, v, ch)
				}

				// Put data into allRData
				go func() {
					ich := fanInChannel(mchans)
					for p := range ich {
						key := p.First
						val := p.Second
						h.Write([]byte(key))
						// Hash key to get worker rank to set as reducer
						redRank := int(h.Sum32())%(len(NodesMap)-1) + 1
						h.Reset()
						// Put calculated rank and data into allRData
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
					}
					// we can detect when they're done
					mapDoneChan <- true
				}()

				fmt.Println(md.m)

			case EndOfMapMSG:
				// Wait for completion of all Map Tasks
				for i := 0; i < numMapData; i++ {
					<-mapDoneChan
				}

				// Collect reader information and send to master

				// Collect data
				var m2r MapperToReducersInfo
				m2r.mapper = MyRank
				m2r.reducers = make([]int, len(allRData))
				i := 0
				for k, _ := range allRData {
					m2r.reducers[i] = k
					i = i + 1
				}

				// Connect to master
				masterAddr := NodesMap[0]
				var mc net.Conn
				if mc, err = net.Dial("tcp", masterAddr); err != nil {
					log.Fatal(err)
				}

				// Send reduce workers message to master
				mcb := bufio.NewWriter(mc)
				if err = mcb.WriteByte(ReduceWorkersMSG); err != nil {
					log.Fatal(err)
				}

				// Send reduce worker list to master
				enc := json.NewEncoder(mc)
				if err = enc.Encode(&m2r); err != nil {
					log.Fatal(err)
				}

				mc.Close()

			case ReduceMSG:
				// Get list of IWs to get data from
				numRedData = numRedData + 1
				var iwr IWRanks
				dec := json.NewDecoder(b)
				if err := dec.Decode(&iwr); err != nil {
					log.Fatal(err)
				}
				fmt.Println(iwr.ranks)
				// Initiate connection with all IWs, collect data & start reducers.

				var rdatamap map[string][]string // accumulated data for reducers
				for rank := range iwr.ranks {

					var redd ReduceData // data from each IW
					if rank != MyRank { // If i have data, just copy it over
						iwaddr := NodesMap[rank]
						var iwc net.Conn
						if iwc, err = net.Dial("tcp", iwaddr); err != nil {
							log.Fatal(err)
						}
						// Send request data to IW
						iwcb := bufio.NewWriter(iwc)
						if err = iwcb.WriteByte(IWDataMSG); err != nil {
							log.Fatal(err)
						}
						// Send rank
						var rd RequestData
						rd.rank = MyRank
						enc := json.NewEncoder(iwc)
						if err = enc.Encode(&rd); err != nil {
							log.Fatal(err)
						}
						// Expect data on same channel
						dec := json.NewDecoder(iwc)
						if err = dec.Decode(&redd); err != nil {
							log.Fatal(err)
						}
						iwc.Close()
					} else {
						redd.m = allRData[MyRank]
					}

					// accumulate data
					for k, v := range redd.m {
						lst, ok := rdatamap[k]
						if !ok {
							lst = make([]string, 0)
						}
						lst = append(lst, v...)
						rdatamap[k] = lst
					}
				}
				// Start reduce jobs
				rchans := make(map[string]chan Pair)
				for k, v := range rdatamap {
					ch := make(chan Pair)
					rchans[k] = ch

					go func(k string, v []string, ch chan Pair) {
						mr.Reducer(k, v, ch)
						close(ch)
					}(k, v, ch)
				}

				go func() {
					ich := fanInChannel(rchans)
					var rd ReducedData
					rd.reducer = MyRank
					rd.data = make([]Pair, 0)
					for p := range ich {
						rd.data = append(rd.data, p)
					}

					// Send data to master
					masterAddr := NodesMap[0]
					var mc net.Conn
					if mc, err = net.Dial("tcp", masterAddr); err != nil {
						log.Fatal(err)
					}
					// Send request data to IW
					mcb := bufio.NewWriter(mc)
					if err = mcb.WriteByte(ReducedDataMSG); err != nil {
						log.Fatal(err)
					}
					// Send reduced data

					enc := json.NewEncoder(mc)
					if err = enc.Encode(&rd); err != nil {
						log.Fatal(err)
					}
					redDoneChan <- true
				}()

			case IWDataMSG:
				// Read rank of IW
				var rd RequestData
				dec := json.NewDecoder(b)
				if err := dec.Decode(&rd); err != nil {
					log.Fatal(err)
				}
				log.Printf("Sending data to %d\n", rd.rank)
				// Send data to IW
				reduceData := allRData[rd.rank]
				var redd ReduceData
				redd.m = reduceData
				enc := json.NewEncoder(c)
				if err := enc.Encode(&redd); err != nil {
					log.Fatal(err)
				}

			case EndLifeMSG:
				// Wait for all reduce jobs to finish
				for i := 0; i < numRedData; i++ {
					<-redDoneChan
				}
				// End the life of the worker
				state = SW

			default:
				log.Fatal("Not a supported message type... exiting!")
			}
			c.Close()
		case SW:
			os.Exit(0)
		}

	}

}

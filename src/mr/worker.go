package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
)

const (
	IW = iota // Initialized worker
	UW = iota // Unintialized worker
	SW = iota // Stopped worker
)

// Map of reducer num to reducer data.
// Needs to be initialized and protected from concurrent access by mappers.
var allRData map[int]map[string][]string 

func RunWorker(mr MapReduce) {

	state := UW	// Initially all workers are uninitialized

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
			c, err := l.Accept();
			if  err != nil {
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
				// Get Map Data to start map tasks
				var md MapData
				dec := json.NewDecoder(b)
				if err := dec.Decode(&md); err != nil {
					log.Fatal(err)
				}
				// TODO Start map tasks with incoming
				// TODO pass map tasks a channel so that
				// we can detect when they're done
				fmt.Println(md.m)

			case EndOfMapMSG:
				// Wait for completion of all Map Tasks
				// Collect reader information and send to master
				// TODO

			case ReduceMSG:
				// Get list of IWs to get data from
				var iwr IWRanks
				dec := json.NewDecoder(b)
				if err := dec.Decode(&iwr); err != nil {
					log.Fatal(err)
				}
				fmt.Println(iwr.ranks)
				// Initiate connection with all IWs, collect data & start reducers.
				
				var rdatamap map[string][]string	// accumulated data for reducers
				for rank := range iwr.ranks {		
				
					var redd ReduceData		// data from each IW
					if rank != MyRank {		// If i have data, just copy it over
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
				// TODO Start reduce jobs
				// TODO Send data to master	

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

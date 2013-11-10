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
	IM = iota // Initialized master
	UM = iota // Unintialized master
	SM = iota // Stopped master
)

var workingMappers map[int]bool  // Set of mappers assigned map data to
var workingReducers map[int]bool // Set of reducers assigned reduce data to
var inReducer map[int][]int      // List of mappers that need to send data to a reducer

func RunServer() {

	state := UM

	if MyRank != 0 {
		log.Fatal("Master should be rank 0")
	}

	// Send init message to all workers in NodeMap
	for k, v := range NodesMap {
		fmt.Printf("Connecting to %d at address %s\n", k, v)
		c, err := net.Dial("tcp", v)
		if err != nil {
			log.Fatal(err)
		}
		c.Close()
	}

	// Send Map Data to every Mapper
	// TODO Split Data & Send to as many mappers as data and not all
	// TODO Assign set of working mappers to "workingMappers"
	for _, v := range NodesMap {
		var iwc net.Conn
		var err error

		if iwc, err = net.Dial("tcp", v); err != nil {
			log.Fatal(err)
		}

		// Send "Map" message to every worker
		iwcb := bufio.NewWriter(iwc)
		if err = iwcb.WriteByte(MapMSG); err != nil {
			log.Fatal(err)
		}

		// Send map data to worker
		var md MapData
		// TODO - Split data and put into "md"
		enc := json.NewEncoder(iwc)
		if err = enc.Encode(&md); err != nil {
			log.Fatal(err)
		}

		// Send "End of Map" data to every worker
		if err = iwcb.WriteByte(EndOfMapMSG); err != nil {
			log.Fatal(err)
		}
		iwc.Close()
	}

	// Switch state to listen on messages from workers
	state = IM

	l, err := net.Listen("tcp", MyIP)
	if err != nil {
		log.Fatal(err)
	}

	for {
		switch state {
		case IM:
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

			fmt.Printf("M : Received message - %d\n", mode)

			switch mode {
			case ReduceWorkersMSG:

				if len(workingMappers) == 0 {
					log.Fatal("Got a set of reducers from a mapper worker when remaining workers to send me data is 0 !")
				}

				// Get reducers from all mappers
				var m2r MapperToReducersInfo
				dec := json.NewDecoder(b)
				if err := dec.Decode(&m2r); err != nil {
					log.Fatal(err)
				}
				// combine into convenient Data structure
				for r := range m2r.reducers {
					lst, ok := inReducer[r]
					if !ok {
						lst = make([]int, 0)
					}
					lst = append(lst, m2r.mapper)
					inReducer[r] = lst
				}

				delete(workingMappers, m2r.mapper)

				// When last mapper sends data, ask workers to do reduce
				if len(workingMappers) == 0 {

					// Initialize working reducers
					workingReducers = make(map[int]bool)
					// Send Reducers info about which mappers to get data from
					// (k is the rank of the reducer while v is the set of workers to get data from)
					for k, v := range inReducer {
						// Connect to reducers and send them the
						// list of mappers to get data from
						ip := NodesMap[k]
						var rc net.Conn
						if rc, err = net.Dial("tcp", ip); err != nil {
							log.Fatal(err)
						}
						// Send "Reduce" message to reducers
						rcb := bufio.NewWriter(rc)
						if err = rcb.WriteByte(ReduceMSG); err != nil {
							log.Fatal(err)
						}
						// Send ranks of mappers to get data from
						var iwr IWRanks
						iwr.ranks = v
						enc := json.NewEncoder(rc)
						if err = enc.Encode(&iwr); err != nil {
							log.Fatal(err)
						}
						// Add to the set of working reducers
						workingReducers[k] = true
						rc.Close()
					}
				}

			case ReducedDataMSG:
				if len(workingReducers) == 0 {
					log.Fatal("Got reduced data from a reducer when I've already received all data !")
				}

				// Get data from mapper
				var rd ReducedData
				dec := json.NewDecoder(b)
				if err := dec.Decode(&rd); err != nil {
					log.Fatal(err)
				}

				// Print final reduced data - different thread ? to a file ?
				for k, v := range rd.m {
					fmt.Println(k + "\t" + v)
				}

				// Send "End life" message to that reducer
				ip := NodesMap[rd.reducer]
				var rc net.Conn
				if rc, err = net.Dial("tcp", ip); err != nil {
					log.Fatal(err)
				}
				rcb := bufio.NewWriter(rc)
				if err = rcb.WriteByte(EndLifeMSG); err != nil {
					log.Fatal(err)
				}
				rc.Close()
				// When last mapper sends data
				// switch state to SM

				delete(workingReducers, rd.reducer)

				if len(workingReducers) == 0 {
					state = SM
				}

			}
			c.Close()
		case SM:
			// TODO Maybe do the printing from here?
			os.Exit(0)
		}
	}
}

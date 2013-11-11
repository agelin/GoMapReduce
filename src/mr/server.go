package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	IM = iota // Initialized master
	UM = iota // Unintialized master
	SM = iota // Stopped master
)

var workingMappers map[int]bool  // Set of mappers assigned map data to
var workingReducers map[int]bool // Set of reducers assigned reduce data to
var inReducer map[int][]int      // List of mappers that need to send data to a reducer

func SplitData(data string, splitConf SplitConf) map[int]string {

	fd := FileSplitter(data, splitConf) // Call to Split file in order keep in sync with single-node implementation

	numw := len(NodesMap) - 1
	ld := len(fd)         // lines of data
	spm := int(ld / numw) // Splits per map

	if spm == 0 && ld > 0 {
		spm = 1
	}

	m2d := make(map[int]string)

	m2d[0] = "Wrong map call; You called Server Rank"

	for i, j := 0, 0; i < len(fd); i, j = i+spm, (j+1)%numw {
		if i+spm < len(fd) {
			m2d[j+1] = strings.Join(fd[i:i+spm], "\n")
		} else {
			m2d[j+1] = strings.Join(fd[i:], "\n")
		}
	}
	return m2d
}

func RunServer(inputdir string) {

	state := UM

	// Setting standard logger

	lg, err := os.OpenFile("logfile", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(lg)

	if MyRank != 0 {
		log.Fatal("Master should be rank 0")
	}

	// Send init message to all workers in NodeMap
	for k, v := range NodesMap {
		log.Printf("Connecting to %d at address %s\n", k, v)
		c, err := net.Dial("tcp", v)
		if err != nil {
			log.Fatal(err)
		}
		c.Close()
	}

	// Split Data
	// make sure the directory exists
	files, err := ioutil.ReadDir(inputdir)
	if err != nil {
		log.Printf("could not read files in directory ", inputdir, ", err:", err)
		log.Fatal(err)
	}

	// Send Map Data to every Mapper
	// TODO Split Data & Send to as many mappers as data and not all -Done
	// TODO Assign set of working mappers to "workingMappers"

	for _, f := range files {
		if !f.IsDir() {

			fullPath := inputdir + "/" + f.Name()
			data, err := ioutil.ReadFile(fullPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "could not read file, err:", err)
				os.Exit(-1)
			}
			// Call Splitter for each file. Split each file across all available Mapppers
			splitConf := SplitConf{"\n", 200} // Configure the Splitter i.e., seperator and count
			md := SplitData(string(data), splitConf)

			for mr, v := range md {

				var iwc net.Conn
				var err error

				iwip := NodesMap[mr]
				if iwc, err = net.Dial("tcp", iwip); err != nil {
					log.Fatal(err)
				}
				// Send "Map" message to every worker
				iwcb := bufio.NewWriter(iwc)
				if err = iwcb.WriteByte(MapMSG); err != nil {
					log.Fatal(err)
				}

				workingMappers[mr] = true

				// Send map data to worker
				// Split data and put into "md"

				var md MapData
				md.m = make(map[string]string)
				key := f.Name() + "$" + strconv.Itoa(mr) // $ can be latter used to split

				md.m[key] = v

				enc := json.NewEncoder(iwc)
				if err = enc.Encode(&md); err != nil {
					log.Fatal(err)
				}
				iwc.Close()
			}
		}

		// Send "End of Map" data to every worker
		for mr, _ := range workingMappers {
			var iwc net.Conn
			var err error

			iwip := NodesMap[mr]
			if iwc, err = net.Dial("tcp", iwip); err != nil {
				log.Fatal(err)
			}

			// Send "End of Map" Message
			iwcb := bufio.NewWriter(iwc)
			if err = iwcb.WriteByte(EndOfMapMSG); err != nil {
				log.Fatal(err)
			}
		}
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

			log.Printf("M : Received message - %d\n", mode)

			switch mode {
			case ReduceWorkersMSG:

				if len(workingMappers) == 0 {
					log.Fatalf("Got a set of reducers from a mapper worker when remaining workers to send me data is 0 !")
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
					log.Fatalf("Got reduced data from a reducer when I've already received all data !")
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

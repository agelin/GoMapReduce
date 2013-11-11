package mr

import (
	"bufio"
	"encoding/json"
	"io"
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

func RunServer(inputdir string, output io.Writer) {

	// Initialize data structures
	workingReducers = make(map[int]bool) // Initialize working reducers
	workingMappers = make(map[int]bool)  // Initialize working mappers
	inReducer = make(map[int][]int)      // Initialize set of mappers from which a reducer should get data

	state := UM

	if MyRank != 0 {
		log.Fatal("Master should be rank 0")
	}

	// Send init message to all workers in NodeMap
	for k := 1; k < len(NodesMap); k++ {
		v := NodesMap[k]
		//for k, v := range NodesMap {
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
	// Split Data & Send to as many mappers as data and not all
	// Assign set of working mappers to "workingMappers"

	for _, f := range files {
		if !f.IsDir() {

			fullPath := inputdir + "/" + f.Name()
			data, err := ioutil.ReadFile(fullPath)
			if err != nil {
				log.Fatal("Could not read file, err:", err)
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
				//iwcb := bufio.NewWriter(iwc)

				var msg ActionMessage
				msg.Msg = MapMSG
				menc := json.NewEncoder(iwc)
				if err = menc.Encode(&msg); err != nil {
					log.Fatal(err)
				}

				workingMappers[mr] = true

				// Send map data to worker
				// Split data and put into "md"

				var md MapData
				md.M = make(map[string]string)
				key := f.Name() + "$" + strconv.Itoa(mr) // $ can be latter used to split

				md.M[key] = v

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
			//			iwcb := bufio.NewWriter(iwc)
			//			if err = iwcb.WriteByte(EndOfMapMSG); err != nil {
			//				log.Fatal(err)
			//			}
			var msg ActionMessage
			msg.Msg = EndOfMapMSG
			menc := json.NewEncoder(iwc)
			if err = menc.Encode(&msg); err != nil {
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
			var mode string
			//			if mode, err = b.ReadByte(); err != nil {
			//				log.Fatal(err)
			//			}
			var msg ActionMessage
			mdec := json.NewDecoder(c)
			if err := mdec.Decode(&msg); err != nil {
				log.Fatal(err)
			}
			mode = msg.Msg
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
				for r := range m2r.Reducers {
					lst, ok := inReducer[r]
					if !ok {
						lst = make([]int, 0)
					}
					lst = append(lst, m2r.Mapper)
					inReducer[r] = lst
				}

				delete(workingMappers, m2r.Mapper)

				// When last mapper sends data, ask workers to do reduce
				if len(workingMappers) == 0 {

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
						//						rcb := bufio.NewWriter(rc)
						//						if err = rcb.WriteByte(ReduceMSG); err != nil {
						//							log.Fatal(err)
						//						}

						var msg ActionMessage
						msg.Msg = ReduceMSG
						menc := json.NewEncoder(rc)
						if err = menc.Encode(&msg); err != nil {
							log.Fatal(err)
						}

						// Send ranks of mappers to get data from to reducers
						var iwr IWRanks
						iwr.Ranks = v
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

				for _, p := range rd.Data {
					k := p.First
					v := p.Second
					//fmt.Println(k + "\t" + v)
					toprint := k + "\t" + v
					output.Write([]byte(toprint))
				}

				// Send "End life" message to that reducer
				ip := NodesMap[rd.Reducer]
				var rc net.Conn
				if rc, err = net.Dial("tcp", ip); err != nil {
					log.Fatal(err)
				}
				//				rcb := bufio.NewWriter(rc)
				//				if err = rcb.WriteByte(EndLifeMSG); err != nil {
				//					log.Fatal(err)
				//				}
				var msg ActionMessage
				msg.Msg = EndLifeMSG
				menc := json.NewEncoder(rc)
				if err = menc.Encode(&msg); err != nil {
					log.Fatal(err)
				}
				rc.Close()
				// When last mapper sends data
				// switch state to SM

				delete(workingReducers, rd.Reducer)

				if len(workingReducers) == 0 {
					state = SM
				}
			default:
				log.Println("Master received unexpected message...")
			}
			c.Close()
			
			
		case SM:
			// TODO Maybe do the printing from here?
			os.Exit(0)
		}
	}
}

// Divides data and assigns them to mappers. Information returned in a map[int]string
func SplitData(data string, splitConf SplitConf) map[int]string {

	fd := FileSplitter(data, splitConf) // Call to Split file in order keep in sync with single-node implementation
	numw := len(NodesMap) - 1
	ld := len(fd)         // lines of data
	spm := int(ld / numw) // Splits per map
	if spm == 0 && ld > 0 {
		spm = 1
	}
	m2d := make(map[int]string)
	for i, j := 0, 0; i < len(fd); i, j = i+spm, (j+1)%numw {
		if i+spm < len(fd) {
			m2d[j+1] = strings.Join(fd[i:i+spm], "\n")
		} else {
			m2d[j+1] = strings.Join(fd[i:], "\n")
		}
	}
	return m2d
}

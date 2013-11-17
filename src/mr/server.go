package mr

import (
    "encoding/json"
    "io"
    "io/ioutil"
    "log"
    "net"
    "strconv"
    "strings"
)

var workingMappers map[int]bool  // Set of mappers assigned map data to
var workingReducers map[int]bool // Set of reducers assigned reduce data to
var inReducer map[int][]int      // List of mappers that need to send data to a reducer

func RunServer(inputdir string, output io.Writer, quitServer chan bool) {

    // Initialize data structures
    workingReducers = make(map[int]bool) // Initialize working reducers
    workingMappers = make(map[int]bool)  // Initialize working mappers
    inReducer = make(map[int][]int)      // Initialize set of mappers from which a reducer should get data

    if MyRank != 0 {
        log.Fatal("Master should be rank 0")
    }

    // Send init message to all workers in NodeMap
    for k := 1; k < len(NodesMap); k++ {
        v := NodesMap[k]
        log.Printf("M : Connecting to worker %d at address %s\n", k, v)
        c, err := net.Dial("tcp", v)
        if err != nil {
            log.Fatal(err)
        }
        if err := c.Close(); err != nil {
            log.Fatal(err)
        }
    }
    log.Printf("M : All workers initialized\n")

    // Split Data
    // make sure the directory exists
    files, err := ioutil.ReadDir(inputdir)
    if err != nil {
        log.Printf("could not read files in directory ", inputdir, ", err:", err)
        log.Fatal(err)
    }
    log.Printf("M : %d files in input directory\n", len(files))

    // Send Map Data to every Mapper
    // Split Data & Send to as many mappers as data and not all
    // Assign set of working mappers to "workingMappers"

    for _, f := range files {
        if !f.IsDir() {
            log.Printf("M : Splitting data for file %s\n", f.Name())

            fullPath := inputdir + "/" + f.Name()
            data, err := ioutil.ReadFile(fullPath)
            if err != nil {
                log.Fatal("Could not read file, err:", err)
            }
            // Call Splitter for each file. Split each file across all available Mapppers
            splitConf := SplitConf{"\n", 200} // Configure the Splitter i.e., seperator and count
            md := SplitData(string(data), splitConf)

            log.Printf("M : Data split for file %s\n", f.Name())

            for mr, v := range md {

                var iwc net.Conn
                var err error

                iwip := NodesMap[mr]
                if iwc, err = net.Dial("tcp", iwip); err != nil {
                    log.Fatal(err)
                }

                // Send "Map" message to every worker
                var msg ActionMessage
                msg.Msg = MapMSG
                menc := json.NewEncoder(iwc)
                if err = menc.Encode(&msg); err != nil {
                    log.Fatal(err)
                }

                log.Printf("M : Sent \"MapMSG\" to worker %d\n", mr)

                workingMappers[mr] = true

                // Send map data to worker
                // Split data and put into "md"

                var md MapData
                md.M = make(map[string]string)
                key := f.Name() + "$" + strconv.Itoa(mr) // $ can be later used to split

                md.M[key] = v

                //Debuggin JSON data
                //				denc := json.NewEncoder(os.Stdout)
                //				if err = denc.Encode(&md); err != nil {
                //					log.Fatal(err)
                //				}

                enc := json.NewEncoder(iwc)
                if err = enc.Encode(&md); err != nil {
                    log.Fatal(err)
                }

                log.Printf("M : Sent map data to worker %d\n", mr)

                if err := iwc.Close(); err != nil {
                    log.Fatal(err)
                }
            }
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

        var msg ActionMessage
        msg.Msg = EndOfMapMSG
        menc := json.NewEncoder(iwc)
        if err = menc.Encode(&msg); err != nil {
            log.Fatal(err)
        }
        log.Printf("M : Sending \"EndOfMapMSG\" to worker %d\n", mr)
    }

    /**************************************************************/
    /* 				Done sending all map data to workers          */
    /**************************************************************/

    // Switch state to listen on messages from workers

    l, err := net.Listen("tcp", MyIP)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("M : Listening at IP : %s\n", MyIP)

    for {

        var c net.Conn
        var err error
        if c, err = l.Accept(); err != nil {
            log.Fatal(err)
        }

        go func(c net.Conn) {

            var mode string
            var msg ActionMessage
            dec := json.NewDecoder(c)
            if err := dec.Decode(&msg); err != nil {
                log.Fatal(err)
            }
            mode = msg.Msg
            log.Printf("M : Received message - %d\n", mode)

            switch mode {
            case ReduceWorkersMSG:

                log.Printf("M : Got message \"ReduceWorkersMSG\"\n")

                if len(workingMappers) == 0 {
                    log.Fatalf("Got a set of reducers from a mapper worker when remaining workers to send me data is 0 !")
                }

                // Get reducers from all mappers
                var m2r MapperToReducersInfo
                //				dec := json.NewDecoder(c)
                if err := dec.Decode(&m2r); err != nil {
                    log.Fatal(err)
                }

                log.Printf("M : Got all reducers to which worker %d needs to send data\n", m2r.Mapper)

                // combine into convenient Data structure
                for _, r := range m2r.Reducers {
                    lst, ok := inReducer[r]
                    if !ok {
                        lst = make([]int, 0)
                    }
                    lst = append(lst, m2r.Mapper)
                    inReducer[r] = lst
                }

                log.Printf("M : Added worker %d to all reducer workers to which it will send data\n", m2r.Mapper)

                delete(workingMappers, m2r.Mapper)

                log.Printf("M : Deleted mapper %d from set of working mappers \n", m2r.Mapper)

                // When last mapper sends data, ask workers to do reduce
                if len(workingMappers) == 0 {

                    log.Printf("M : Deleted LAST mapper (%d) from set of working mappers \n", m2r.Mapper)

                    // Send Reducers info about which mappers to get data from
                    // (k is the rank of the reducer while v is the set of workers to get data from)
                    for k, v := range inReducer {
                        // Connect to reducers and send them the
                        // list of mappers to get data from

                        if k == 0 {
                            log.Fatal("Sending ReduceMSG to master from master !")
                        }

                        ip := NodesMap[k]
                        var rc net.Conn
                        if rc, err = net.Dial("tcp", ip); err != nil {
                            log.Fatal(err)
                        }
                        var msg ActionMessage
                        msg.Msg = ReduceMSG
                        menc := json.NewEncoder(rc)
                        if err = menc.Encode(&msg); err != nil {
                            log.Fatal(err)
                        }

                        log.Printf("M : Sent \"ReduceMSG\" to worker %d\n", k)

                        // Send ranks of mappers to get data from to reducers
                        var iwr IWRanks
                        iwr.Ranks = v
                        enc := json.NewEncoder(rc)
                        if err = enc.Encode(&iwr); err != nil {
                            log.Fatal(err)
                        }

                        log.Printf("M : Sent set of worker ranks to get reducer input from to worker %d\n", k)

                        // Add to the set of working reducers
                        workingReducers[k] = true

                        log.Printf("M : Added worker %d to the set of working reducers\n", k)

                        if err := rc.Close(); err != nil {
                            log.Fatal(err)
                        }
                    }
                }

            case ReducedDataMSG:
                log.Printf("M : Got message \"ReducedDataMSG\"\n")

                if len(workingReducers) == 0 {
                    log.Fatalf("Got reduced data from a reducer when I've already received all data !")
                }

                // Get data from mapper
                var rd ReducedData
                //				dec := json.NewDecoder(c)
                if err := dec.Decode(&rd); err != nil {
                    log.Fatal(err)
                }

                log.Printf("M : Got ReducedData instance from %d\n", rd.Reducer)

                // Print final reduced data - different thread ? to a file ?

                for _, p := range rd.Data {
                    k := p.First
                    v := p.Second
                    //fmt.Println(k + "\t" + v)
                    toprint := k + "\t" + v + "\n"
                    output.Write([]byte(toprint))
                }

                log.Printf("M : Ouput reduced data from worker %d to io.Writer instance \n", rd.Reducer)


                // When last mapper sends data
                // switch state to SM
                delete(workingReducers, rd.Reducer)

                log.Printf("M : Deleted reducer %d from set of working reducers\n", rd.Reducer)

                if len(workingReducers) == 0 {
                    log.Printf("M : Received reduced data from all reducers, sending end of life message to all workers, switching state to SM\n")

                    // Send "End life" message to that reducer
                    for k, v := range NodesMap {
                        ip := v
                        var rc net.Conn
                        if rc, err = net.Dial("tcp", ip); err != nil {
                            log.Fatal(err)
                        }
                        var msg ActionMessage
                        msg.Msg = EndLifeMSG
                        menc := json.NewEncoder(rc)
                        if err = menc.Encode(&msg); err != nil {
                            log.Fatal(err)
                        }

                        log.Printf("M : Sent \"EndLifeMSG\" to worker %d\n", rd.Reducer)

                        if err := rc.Close(); err != nil {
                            log.Fatal(err)
                        }
                    }



                    /**************************************************************/
                    /*					End life of server					      */
                    /**************************************************************/
                    log.Printf("M : Stopping Master\n")
                    quitServer <- true
                }
            default:
                log.Fatal("Master received unexpected message...")
            }
            if err := c.Close(); err != nil {
                log.Fatal(err)
            }
        }(c)

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

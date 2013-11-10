package mr

import("net"
		"log"
		"fmt"
		"bufio"
		"encoding/json"
		"os"
		)


const (
	IM = iota // Initialized master
	UM = iota // Unintialized master
	SM = iota // Stopped master
)


func RunServer(){

	state := UM

	if MyRank != 0 {
		log.Fatal ("Master should be rank 0")
	}

	// Send init message to all workers in NodeMap
	for k, v := range NodesMap{
		fmt.Printf("Connecting to %d at address %s\n", k, v) 
		c, err := net.Dial("tcp", v); 
		if err != nil{
			log.Fatal(err)
		}
		c.Close()
	}
	
	// Send Map Data to every worker
	// TODO Split Data
	for k, v := range NodesMap{
		var iwc net.Conn
		var err error
		
		if iwc, err = net.Dial("tcp", v); err != nil {
			log.Fatal(err)
		}
		// Send map data to worker
		iwcb := bufio.NewWriter(iwc)
		if err = iwcb.WriteByte(Map); err != nil {
			log.Fatal(err)
		}
		
		var md MapData
		enc := json.NewEncoder(iwc)
		if err = enc.Encode(&md); err != nil {
			log.Fatal(err)
		}
	
	
	// Send end of Map data to every worker
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
				case ReduceWorkers:
					// TODO
					// Get reducers from all mappers
					// combine into convenient Data structure
					
					// When last mapper sends data,
					// Send Reducers info about which mappers to get data from
				
				case ReducedData:
					// Get data from mapper
					// Send end life to that mapper
					
					// When last mapper sends data
					// switch state to SM
				
			}
		 
		case SM:
			os.Exit(0)
		}
	}
}
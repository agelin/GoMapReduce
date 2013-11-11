package mr


// This file contains communication specific data structures

const (
	_                string = "a"
	MapMSG           string = "b"	// Map data & task [master -> mapper]
	EndOfMapMSG      string = "c"	// End mapper and send master data [master -> mapper]
	ReduceMSG        string = "d"	// 
	IWDataMSG        string = "e"
	EndLifeMSG       string = "f"
	ReduceWorkersMSG string = "g"	// Set of reducers from a mappers to which it will send data [mapper -> master]
	ReducedDataMSG   string = "h"
)

type ActionMessage struct {
	Msg string
}

// Sent master -> mappers
type MapData struct {
	M map[string]string // Key Value Pair
}

// Sent mappers -> master
type MapperToReducersInfo struct {
	Mapper   int   // Rank of mapper
	Reducers []int // Ranks of reducers this mappers needs to send data to
}

// Sent mappers -> reducers
type ReduceData struct {
	M map[string][]string
}

// Sent reducers -> master
type ReducedData struct {
	Reducer int    // Rank of reducer
	Data    []Pair // reduced data
}

// Sent master -> reducers
type IWRanks struct {
	Ranks []int // Rank of IW to get data from
}

// Send reducers -> mappers (request for data)
type RequestData struct {
	Rank int // Rank of requesting IW
}

var MyIP = "locahost:4000"  // My Ip address
var MyRank = 1              // My Rank
var NodesMap map[int]string // Node rank to ip:port, rank 0 is master

// Sends data over JSON to connected node
//func sendJSON(c net.Conn, data interface{}) {
//	enc := json.NewEncoder(c)
//	if err = enc.Encode(&data); err != nil {
//		log.Fatal(err)
//	}
//}
//
//func getFromJSON(c net.Conn ) interface{} {
//	dec := json.NewDecoder(b)
//	if err := dec.Decode(&rd); err != nil {
//		log.Fatal(err)
//	}
//	return rd
//}

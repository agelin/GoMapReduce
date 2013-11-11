package mr

// This file contains communication specific data structures

const (
	_                byte = 0xA1
	MapMSG           byte = 0xA2
	EndOfMapMSG      byte = 0xA3
	ReduceMSG        byte = 0xA4
	IWDataMSG        byte = 0xA5
	EndLifeMSG       byte = 0xA6
	ReduceWorkersMSG byte = 0xA7
	ReducedDataMSG   byte = 0xA8
)

// Sent master -> mappers
type MapData struct {
	m map[string]string // Key Value Pair
}

// Sent mappers -> master
type MapperToReducersInfo struct {
	mapper   int   // Rank of mapper
	reducers []int // Ranks of reducers this mappers needs to send data to
}

// Sent mappers -> reducers
type ReduceData struct {
	m map[string][]string
}

// Sent reducers -> master
type ReducedData struct {
	reducer int    // Rank of reducer
	data    []Pair // reduced data
}

// Sent master -> reducers
type IWRanks struct {
	ranks []int // Rank of IW to get data from
}

// Send reducers -> mappers (request for data)
type RequestData struct {
	rank int // Rank of requesting IW
}

var MyIP = "locahost:4000"  // My Ip address
var MyRank = 1              // My Rank
var NodesMap map[int]string // Node rank to ip:port, rank 0 is master

package mr

const (
	_        byte = iota
	Map      byte = iota
	EndOfMap byte = iota
	Reduce   byte = iota
	IWData   byte = iota
	EndLife  byte = iota
)

type MapData struct {
	m map[string]string // Key Value Pair
}

type ReduceData struct {
	m map[string][]string
}

type IWRanks struct {
	ranks []int // Rank of IW to get data from
}

type RequestData struct {
	rank int // Rank of requesting IW
}

var MyIP = "locahost:4000"               // My Ip address
var MyRank = 1                           // My Rank
var NodesMap map[int]string              // Node rank to ip:port, rank 0 is master
var RDataMap map[int]map[string][]string // Map of reducer num to reducer data

package mr

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type Pair struct {
	First  string
	Second string
}

// Spitter Configuration
type SplitConf struct {
	sep   string // Seperator
	count int    // Number of Seperator's after which a split will happen
}

const (
	MapBuff = 100
)

type MapReduce interface {
	Mapper(key, value string, out chan Pair)
	Reducer(key string, value []string, out chan Pair)
}

func FileSplitter(str string, conf SplitConf) []string {
	sep := conf.sep
	counter := 0
	start := 0
	j := 0
	n := strings.Count(str, sep)/conf.count + 1
	newStr := make([]string, n)

	for i := 0; i < len(str); i++ {
		if str[i] == sep[0] {
			counter++
		}

		if counter == conf.count {
			newStr[j] = str[start:i]
			start = i + len(sep)
			j++
			counter = 0
		}
	}
	if counter > 0 {
		newStr[j] = str[start : len(str)-1]
	}
	return newStr[0 : j+1]
}

func fanInChannel(m map[string]chan Pair) chan Pair {
	// Fan in pattern from http://talks.golang.org/2012/concurrency.slide#27
	ch := make(chan Pair, MapBuff)

	go func(ch chan Pair) {
		quit := make(chan bool)
		for _, v := range m {
			go func(v chan Pair) {
				for d := range v {
					ch <- d
				}
				quit <- true
			}(v)
		}
		for i := 0; i < len(m); i++ {
			//fmt.Println(i)
			<-quit
		}
		close(ch)
	}(ch)
	return ch
}

// For command line arguments
var (
	rank   = flag.Int("rank", -1, "Rank of this node, 0 is master, others are workers")
	config = flag.String("config", "", "configuration file with ranks and ip address of all nodes")
)

// Runs multinode mapreduce and writes output to
// mr - mapreduce instance
// inputdir - directory with input files
func Run(mr MapReduce, inputdir string, output io.Writer) {

	flag.Parse()

	// Read config file
	if *config == "" {
		log.Fatal("No configuration file specified")
	}
	if *rank == -1 {
		log.Fatal("No rank / Invalid rank specified")
	}

	MyRank = *rank

	// Setting standard logger
	logFName := fmt.Sprintf("log%d", MyRank)
	lg, err := os.OpenFile(logFName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(lg)

	fData, err := ioutil.ReadFile(*config)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize NodeMap structure
	NodesMap = make(map[int]string)

	// Reading the configuration file and populating
	// the NodesMap data structure
	// This part is not bullet proof,
	// PLEASE DO NOT GIVE IT ERRONEOUS config files
	scanner := bufio.NewScanner(bytes.NewReader(fData))
	scanner.Split(bufio.ScanLines) // This is default behaviour
	for scanner.Scan() {
		line := scanner.Text()
		lsc := bufio.NewScanner(strings.NewReader(line))
		lsc.Split(bufio.ScanWords)
		// 2 tokens. first is rank, second is ip:port
		lsc.Scan()
		r, err := strconv.Atoi(lsc.Text())
		if err != nil {
			log.Fatal(err)
		}
		lsc.Scan()
		ip := lsc.Text()
		NodesMap[r] = ip
	}

	var ok bool
	MyIP, ok = NodesMap[MyRank]
	if !ok {
		log.Fatal("Could not find my own rank in the configuration file!")
	}

	// If master, Run Server
	if MyRank == 0 {
		RunServer(inputdir, output)
	} else { // Run worker
		RunWorker(mr)
	}
}

// Single Node MR

// Inputs a pointer to a MapReduce object and the input directory
// with the files
/*
func Run(mr MapReduce, inputdir string) chan Pair {

	// make sure the directory exists
	files, err := ioutil.ReadDir(inputdir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "could not read files in directory ", inputdir, ", err:", err)
		os.Exit(-1)
	}

	// Read data from the files and launch mappers
	mappers := make(map[string]chan Pair)
	for _, v := range files {
		if !v.IsDir() {
			fullPath := inputdir + "/" + v.Name()
			//fmt.Println(fullPath)
			data, err := ioutil.ReadFile(fullPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "could not read file, err:", err)
				os.Exit(-1)
			}

			splitConf := SplitConf{"\n", 200} // Configure the Splitter i.e., seperator and count
			mapperData := FileSplitter(string(data), splitConf)

			for i, j := range mapperData {

				mapperName := v.Name() + "$" + strconv.Itoa(i) // $ can be latter used to split
				ch := make(chan Pair)
				mappers[mapperName] = ch

				go func(name string, d string, ch chan Pair) {
					mr.Mapper(name, d, ch)
					close(ch)
				}(mapperName, j, ch)

			}
		}
	}

	// Pipe all data from mappers to intermediate channel
	ich := fanInChannel(mappers)

	// Collect all data from mappers into an intermediate map to send to reducers
	imap := make(map[string][]string)
	for data := range ich {
		k := data.First
		v := data.Second
		//fmt.Println("Key:", k, ", Value:", v)
		lst, ok := imap[k]
		if !ok {
			lst = make([]string, 0)
		}
		lst = append(lst, v)
		imap[k] = lst
		//fmt.Println("Key:", k, ", Value:", lst)
	}

	// Launch reducers, one for each key in the intermediate map
	reducers := make(map[string]chan Pair)
	for k, v := range imap {
		ch := make(chan Pair)
		reducers[k] = ch
		//fmt.Println("Key:", k, ", Value:", v)
		go func(k string, v []string, ch chan Pair) {
			mr.Reducer(k, v, ch)
			close(ch)
		}(k, v, ch)
	}

	och := fanInChannel(reducers)
	return och
}

*/

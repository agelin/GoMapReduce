package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"mr"
	"os"
	"strconv"
	"strings"
	"time"
)

type WC struct{}

// The input key, value will be filename, text
// The output would be word, count
func (wc WC) Mapper(key, value string, out chan mr.Pair) {
	filenameString := strings.Split(key, "$")
	filename := filenameString[0]

	strr := strings.NewReader(value)
	s := bufio.NewScanner(strr)
	s.Split(bufio.ScanWords)
	for s.Scan() {
		word := s.Text()
		out <- mr.Pair{word, filename}
	}
	if err := s.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading file :", err)
		os.Exit(-1)
	}
}

func (wc WC) Reducer(key string, value []string, out chan mr.Pair) {
	fileList := ""
	filemap := make(map[string]int)
	for _, v := range value {
		filemap[v] += 1
	}
	for i, j := range filemap {
		fileList = fileList + " " + i + "-" + strconv.Itoa(j)
	}
	out <- mr.Pair{key, fileList}
}

var (
	inputdir = flag.String("inputdir", ".", "Input directory")
	output   = flag.String("output", "invertedindexoutput", "Output file")
)

func main() {
	wc := WC{}

	o, err := os.Create(*output)
	if err != nil {
		log.Fatal("Could not create output file, err: ", err)
	}

	t0 := time.Now()
	mr.Run(wc, *inputdir, o)
	d := time.Since(t0)
	fmt.Println("GoMapReduce inverted index took " + d.String())
}

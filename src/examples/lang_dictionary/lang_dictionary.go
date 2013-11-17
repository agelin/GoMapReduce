package main

import (
	"bufio"
	"fmt"
	"mr"
	"os"
	"strings"
	"time"
	"flag"
	"log"
)

type WC struct{}

func (wc WC) Mapper(key, value string, out chan mr.Pair) {	
	strr := strings.NewReader(value)
	s := bufio.NewScanner(strr)
	s.Split(bufio.ScanLines)
	
	if err := s.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading file :", err)
		os.Exit(-1)
	}
	for s.Scan() {
		wordandmeaning := strings.Split(s.Text(), "\t")
		if len(wordandmeaning) == 1 {
			continue
		}
		englishword := wordandmeaning[0]
		foreignwords := strings.Split(wordandmeaning[1], ",")
		for _, val := range foreignwords {
			out <- mr.Pair{englishword, val}
		}
	}

}

func (wc WC) Reducer(key string, value []string, out chan mr.Pair) {
	trans := strings.Join(value, "|")

	out <- mr.Pair{key, trans}

}

var (
	inputdir = flag.String("inputdir", ".", "Input directory")
	output   = flag.String("output", "langdictoutput", "Output file")
)

func main() {
	wc := WC{}
    flag.Parse()
	o, err := os.Create(*output)
	if err != nil {
		log.Fatal("Could not create output file, err: ", err)
	}


	t0 := time.Now()
	mr.Run(wc, *inputdir, o)
	d := time.Since(t0)
	fmt.Println("GoMapReduce lang dictionary took " + d.String())

}

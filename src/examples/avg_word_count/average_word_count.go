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
	strr := strings.NewReader(value)
	s := bufio.NewScanner(strr)
	s.Split(bufio.ScanWords)
	for s.Scan() {
		word := s.Text()
		ch := word[0]
		strng := string(ch)
		length := len(word)
		strlng := strconv.Itoa(length)
		out <- mr.Pair{strng, strlng}
	}
	if err := s.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading file :", err)
		os.Exit(-1)
	}
}

// The reducer receives a word, <list of counts>
// It adds up all the counts and outputs a word, combined_count
func (wc WC) Reducer(key string, value []string, out chan mr.Pair) {
	count := 0
	for _, v := range value {
		c, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error converting \"", v, "\" to integer, err:", err)
			os.Exit(-1)
		}
		count += c
	}
	avg := len(value)
	count = count / avg
	out <- mr.Pair{key, strconv.Itoa(count)}
}

var (
	inputdir = flag.String("inputdir", ".", "Input directory")
	output   = flag.String("output", "avgwcoutput", "Output file")
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
	fmt.Println("GoMapReduce average word count took " + d.String())

}

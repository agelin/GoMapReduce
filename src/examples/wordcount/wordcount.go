package main

import (
	"os"
	"bufio"
	"fmt"
	"mr"
	"strings"
	"strconv"
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
		out <- mr.Pair{word, "1"}
		//fmt.Println(word)
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
		//fmt.Println("k: ", key, "v:", v)
		c, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error converting \"", v, "\" to integer, err:", err)
			os.Exit(-1)
		}
		count += c
	}
	out <- mr.Pair{key, strconv.Itoa(count)} 
}

func main() {
	wc := WC{}
	// Ouput all key-value pairs
	out := mr.Run(wc, "Put Your Path here!")
	for p := range out {
		f := p.First
		s := p.Second
		fmt.Println(f, " ", s)
	}
	
}

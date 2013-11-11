package main

import (
	"os"
	"bufio"
	"fmt"
	"mr"
	"strings"
	"strconv"
	"time"
	"flag"
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


var (
	inputdir = flag.String("inputdir", ".", "Input directory")
	)

func main() {
	wc := WC{}
//	of,err := os.Create("/cise/homes/kota/Output123")
//        defer of.Close()
//
//        if err!=nil {
//                return
//        }
	 t0 := time.Now()
	// Ouput all key-value pairs
	mr.Run(wc, *inputdir, os.Stdout)
//	for p := range out {
//                single := p.First + " - " + p.Second
//                of.WriteString(single)
//                of.WriteString("\n")
//        }
        fmt.Print("Time Taken: ")
        fmt.Println(time.Since(t0))
	/*
	for p := range out {
		f := p.First
		s := p.Second
		fmt.Println(f, " ", s)
	}
	*/
	
}

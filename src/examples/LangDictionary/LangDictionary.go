package main

import (
	"bufio"
	"fmt"
	"mr"
	"os"
	"strings"
	"time"
)

type WC struct{}

// The input key, value will be filename, text
// The output would be word, count
func (wc WC) Mapper(key, value string, out chan mr.Pair) {
	//strr := strings.NewReader(value)
	//s := bufio.NewScanner(value)
	//s.Split(bufio.ScanWords)
	
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

// The reducer receives a word, <list of counts>
// It adds up all the counts and outputs a word, combined_count
func (wc WC) Reducer(key string, value []string, out chan mr.Pair) {
	trans := strings.Join(value, "|")

	out <- mr.Pair{key, trans}

}

func main() {
	wc := WC{}
	of, err := os.Create("output")
	defer of.Close()

	if err != nil {
		return
	}

	t0 := time.Now()

	// Ouput all key-value pairs
	out := mr.Run(wc, "input")

	for p := range out {
		translatedline := p.First + "\t" + p.Second
		of.WriteString(translatedline)
		of.WriteString("\n")
	}
	fmt.Print("Time Taken: ")
	fmt.Println(time.Since(t0))

}

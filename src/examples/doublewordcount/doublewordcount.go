package main

import (
	"bufio"
	"fmt"
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
	firstword := ""
	secondword := ""
	word := ""
	temp := ""
	for s.Scan() {
		if firstword != "" {
			firstword = temp

		} else {
			firstword = s.Text()
		}
		secondword = s.Text()
		if secondword != "" {
			word = firstword + " " + secondword
			out <- mr.Pair{word, "1"}
			temp = secondword
		} else {
			break
		}
		//out <- mr.Pair{word, "1"}
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
	of,err := os.Create("/cise/homes/kota/Output12")
        defer of.Close()

        if err!=nil {
                return
        }
	 t0 := time.Now()
	// Ouput all key-value pairs
	out := mr.Run(wc, "/cise/homes/kota/input/")
	for p := range out {
                single := p.First + " - " + p.Second
                of.WriteString(single)
                of.WriteString("\n")
        }
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

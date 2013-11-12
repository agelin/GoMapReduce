package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"mr"
	"os"
	"strings"
	"time"
)

type AG struct{}

func Sort(word string) string {

	byteArr := make([]byte, len(word))
	copy(byteArr[:], word)

	for i := 0; i < len(word)-1; i++ {
		for j := i + 1; j < len(word); j++ {
			if byteArr[i] > byteArr[j] {
				temp := byteArr[i]
				byteArr[i] = byteArr[j]
				byteArr[j] = temp
			}
		}
	}
	return string(byteArr[:])
}

func (ag AG) Mapper(key, val string, out chan mr.Pair) {
	strr := strings.NewReader(val)
	s := bufio.NewScanner(strr)
	s.Split(bufio.ScanWords)
	for s.Scan() {
		inputWord := s.Text()
		word := strings.ToLower(inputWord)
		sortedWord := Sort(word)
		out <- mr.Pair{sortedWord, word}
	}
	if err := s.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading file :", err)
		os.Exit(-1)
	}
}

func (ag AG) Reducer(key string, val []string, out chan mr.Pair) {
	value := strings.Join(val, ",")
	out <- mr.Pair{key, value}
}

var (
	inputdir = flag.String("inputdir", ".", "Input directory")
	output   = flag.String("output", "anagramsoutput", "Output file")
)

func main() {
	ag := AG{}

	o, err := os.Create(*output)
	if err != nil {
		log.Fatal("Could not create output file, err: ", err)
	}

	t0 := time.Now()
	mr.Run(ag, *inputdir, o)
	d := time.Since(t0)
	fmt.Println("GoMapReduce anagrams took " + d.String())
}

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

type MaxTemp struct{}

// The input key, value will be filename, text
// The output would be word, count
func (mt MaxTemp) Mapper(key, value string, out chan mr.Pair) {
	strr := strings.NewReader(value)
	s := bufio.NewScanner(strr)
	s.Split(bufio.ScanLines)
	count := 0
	scannedLine := ""
	maximumTemp := ""
	year := ""

	for s.Scan() {

		count += 1
		scannedLine = s.Text()

		words := strings.Fields(scannedLine)

		maximumTemp = words[18]
		year = words[3]
		year = year[0:4]
		out <- mr.Pair{year, maximumTemp}

	}
	if err := s.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading file :", err)
		os.Exit(-1)
	}
}

// The reducer receives a word, <list of counts>
// It adds up all the counts and outputs a word, combined_count
func (mt MaxTemp) Reducer(key string, value []string, out chan mr.Pair) {

	tempValue := 0.0

	for _, v := range value {
		if v != "\\N" {
			temperature, err := strconv.ParseFloat(v, 64) //strconv.Atoi(v)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error converting \"", v, "\" to float, err:", err)
				os.Exit(-1)
			}

			if temperature > tempValue {
				tempValue = temperature
			}
		}
	}
	out <- mr.Pair{key, strconv.FormatFloat(tempValue, 'g', -1, 64)}
}

var (
	inputdir = flag.String("inputdir", ".", "Input directory")
	output   = flag.String("output", "maxtempoutput", "Output file")
)

func main() {

	mt := MaxTemp{}
    flag.Parse()
	fmt.Println(" ====== GETTING THE MAXIMUM TEMPERATURES FROM THE DATASET ====== ")
	// Ouput all key-value pairs

	o, err := os.Create(*output)
	if err != nil {
		log.Fatal("Could not create output file, err: ", err)
	}

	t0 := time.Now()
	mr.Run(mt, *inputdir, o)
	d := time.Since(t0)
	fmt.Println("GoMapReduce max temp took " + d.String())

}

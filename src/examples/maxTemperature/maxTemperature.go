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

type maxTemp struct{}

// The input key, value will be filename, text
// The output would be word, count
func (maxTemp maxTemp) Mapper(key, value string, out chan mr.Pair) {
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
		//fmt.Println(count, "    ", year, "   ", maximumTemp)

	}
	if err := s.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading file :", err)
		os.Exit(-1)
	}
}

// The reducer receives a word, <list of counts>
// It adds up all the counts and outputs a word, combined_count
func (maxTemp maxTemp) Reducer(key string, value []string, out chan mr.Pair) {

	tempValue := 0.0

	for _, v := range value {
		//fmt.Println("k: ", key, "v:", v)
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
	//out <- mr.Pair{key, strconv.Itoa(tempValue)}
}

func main() {

	t0 := time.Now()
	
	maxTemperature := maxTemp{}
	fmt.Println(" ====== GETTING THE MAXIMUM TEMPERATURES FROM THE DATASET ====== ")
	// Ouput all key-value pairs
	out := mr.Run(maxTemperature, "/home/nitin/cloudAssignment/inputs")

	for p := range out {
		f := p.First
		s := p.Second
		fmt.Println(f, " : ", s)
	}
	
	t1 := time.Now()
	fmt.Printf("The MR call took %v to run.\n", t1.Sub(t0))
}

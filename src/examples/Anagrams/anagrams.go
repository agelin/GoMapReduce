package main

import (
	"os"
	"mr"
	"fmt"
	"bufio"
	"strings"
	"time"
)

type AG struct{}

func Sort(word string) string{
	
	byteArr := make([]byte,len(word))
	copy(byteArr[:],word)
	
	for i:= 0;i < len(word)-1; i++ {
		for j:=i+1;j<len(word);j++ {
			if(byteArr[i] > byteArr[j]) {
				temp := byteArr[i]
				byteArr[i] = byteArr[j]
				byteArr[j] = temp
			}
		}
	}
	return string(byteArr[:])
}

func (ag AG) Mapper(key, val string, out chan mr.Pair ){
	strr := strings.NewReader(val)
	s := bufio.NewScanner(strr)
	s.Split(bufio.ScanWords)
	for s.Scan() {
		inputWord := s.Text()
		word := strings.ToLower(inputWord)
		sortedWord := Sort(word)
		out <- mr.Pair{sortedWord,word}
		}
	if err := s.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading file :", err)
		os.Exit(-1)
	}
}

func (ag AG) Reducer(key string, val []string, out chan mr.Pair){
	value := strings.Join(val,",")
	out <- mr.Pair{key, value} 
}

func main(){
	        ag := AG{}

        of,err := os.Create("/home/jd/Documents/programs/go-lang/scrap/dataset/Output")
        defer of.Close()

        if err!=nil {
                return
        }

        t0 := time.Now()
        out := mr.Run(ag,"/cise/homes/jaideep/Cloud/Hadoop/hadoop-1.2.1/Examples/DataSets/Anagrams/AnagramsInp")

        for p := range out {
                anagramWord := p.First + " - " + p.Second
                of.WriteString(anagramWord)
                of.WriteString("\n")
        }
        fmt.Print("Time Taken: ")
        fmt.Println(time.Since(t0))
}

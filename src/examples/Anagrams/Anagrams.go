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

/*  Argument-1 is Input File Directory
   	Argument-2 is Output File Directory */

func main(){
	    ag := AG{}
			
		ResultsFile := strings.Join([]string{os.Args[2],"/Results"},"")			
		RespTimeFile := strings.Join([]string{os.Args[2],"/ResponseTime"},"")
        of1,err := os.Create(ResultsFile)
        of2,err := os.Create(RespTimeFile)
        
        defer of1.Close()
		defer of2.Close()

        if err!=nil {
                return
        }

        t0 := time.Now()
        out := mr.Run(ag,os.Args[1])	

        for p := range out {
                anagramWord := p.First + " - " + p.Second
                of1.WriteString(anagramWord)
                of1.WriteString("\n")
        }
        timeTaken:= "TimeTaken = " + time.Since(t0).String()  
        of2.WriteString(timeTaken)    
}


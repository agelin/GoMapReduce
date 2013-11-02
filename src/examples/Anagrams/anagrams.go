package main

import (
	"os"
	"mr"
	"fmt"
	"bufio"
	"strings"
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
	out := mr.Run(ag,"/home/jd/Downloads/Cloud/Cloud-Project/DataSets/Anagrams-DataSet/SingleFile")
	
	for p := range out {
		fmt.Println(p.First + " - " + p.Second)	
	}
}

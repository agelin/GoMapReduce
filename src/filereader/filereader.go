package filereader

import (
	"os"
	"fmt"
	"bufio"
)

// Read returns a channel that contains line by line output from the given input file
func Read(fname string) chan string {
	
	out := make(chan string)
	
	// Asynchronously read data from file
	go func () {
	
		// Open the file and defer closing
		f, err := os.Open(fname)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file : %s", fname)
			os.Exit(-1)
		}
		defer f.Close()
		
		// Channel on which data will be output
		s := bufio.NewScanner(f)
		
		for s.Scan() {
	            out <- s.Text()
	    } 
	    
	    if err := s.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "reading file :", err)
		}
		
		close(out)
		return
	}()
	
	return out

}
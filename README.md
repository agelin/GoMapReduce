GoMapReduce
===========

Map Reduce in Go for Cloud Computing Fall'13 @ University of Florida

Single Node & Multinode Mapreduce are implemented in Go.
The MapReduce infrastructure code is provided in src/mr
Some Examples are in src/examples


Quick Setup
-----------
Compile one of the examples (Word Count)

$ export GOPATH=${PWD}
$ go build src/examples/word_count/word_count.go

This will create the word_count executable in the current directory.
Create a config file to run this example on your local machine with the following entries:

0 localhost:56781
1 localhost:56782
2 localhost:56783

Each line is a rank followed by an ip:port pair. There should be no extraneous lines or spaces in this config file. Please do not put a new line after the last entry. 

In 2 different terminals, launch word_count like so:

./word_count -rank=1 -config=config -inputdir=/home/user/input 

./word_count -rank=2 -config=config -inputdir=/home/user/input 


These are the workers and they will wait for the master / server (rank 0) to be spawned.

In these lines "/home/user/input" is the absolute path of the directory where your input files are. The directory is not searched recursively. Also, all text in all the files in this directory must only have UTF-8 characters.

To spawn the server, in a new terminal, say:

./word_count -rank=0 -config=config -inputdir=/home/user/input 

If everything went well, a file by the name "wordcountoutput" will have been created in the working directory of rank 0. Each rank also writes out a log. They are log0, log1, log2 (and so forth). 


Short Description of Working
----------------------------
The single node map reduce reads in data from the specified input directory and launches map goroutines. It collects the output of said goroutines from a fanned-in channel into a map and launches the reducers. The reduced data is collected from a fan-in channel also.

In the multinode map reduce, the "master" reads in the input from the specified directory and sends the data to the "map workers" to begin the map task. Once done, the "map workers" send back a list of "reducer workers" to which they need to send data. The "master" relays this information to the "reducer workers". The "reducer workers" then ask the "mapper workers" for the data and begin reduce tasks. Once they're done, they send back the reduced data to the "master".

The multinode version currently does not implement fault-tolerance, or writing & reading from disk. This means that all the input data, intermediate data and final reduced data must fit into the memory of the "workers" and "master". It also does not sort the intermediate data or the final reduced data.






GoMapReduce
===========

Map Reduce in Go for Cloud Computing Fall'13 @ University of Florida

Single Node & Multinode Mapreduce are implemented in Go.
The MapReduce infrastructure code is provided in src/mr
Some Examples are in src/examples

Short Description of Working
----------------------------
The single node map reduce reads in data from the specified input directory and launches map goroutines. It collects the output of said goroutines from a fanned-in channel into a map and launches the reducers. The reduced data is collected from a fan-in channel also.

In the multinode map reduce, the "master" reads in the input from the specified directory and sends the data to the "map workers" to begin the map task. Once done, the "map workers" send back a list of "reducer workers" to which they need to send data. The "master" relays this information to the "reducer workers". The "reducer workers" then ask the "mapper workers" for the data and begin reduce tasks. Once they're done, they send back the reduced data to the "master".

The multinode version currently does not implement fault-tolerance, or writing & reading from disk. This means that all the input data, intermediate data and final reduced data must fit into the memory of the "workers" and "master". It also does not sort the intermediate data or the final reduced data.



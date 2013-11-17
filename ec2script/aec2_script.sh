#!/bin/bash

# Setup go & download GoMapReduce, compile all executables.
sudo apt-get -y install git
if [ -d GoMapReduce ]; then
    echo "already setup!";
    cd /home/ubuntu/GoMapReduce
    git pull origin master
else
git clone https://github.com/EEL6935022ATeam10/GoMapReduce.git
wget https://go.googlecode.com/files/go1.1.2.linux-amd64.tar.gz
tar xvf go1.1.2.linux-amd64.tar.gz
echo "PATH=$PATH:/home/ubuntu/go/bin" >> ~/.bashrc
echo "export GOROOT=/home/ubuntu/go" >> ~/.bashrc
echo "export GOPATH=/home/ubuntu/GoMapReduce" >> ~/.bashrc
source ~/.bashrc
fi
export PATH=$PATH:/home/ubuntu/go/bin
export GOROOT=/home/ubuntu/go
export GOPATH=/home/ubuntu/GoMapReduce
cd ~/GoMapReduce/bin
go build ../src/examples/anagrams/anagrams.go
go build ../src/examples/avg_word_count/average_word_count.go
go build ../src/examples/double_word_count/double_word_count.go
go build ../src/examples/inverted_index/inverted_index.go 
go build ../src/examples/lang_dictionary/lang_dictionary.go
go build ../src/examples/max_temperature/max_temperature.go
go build ../src/examples/word_count/word_count.go


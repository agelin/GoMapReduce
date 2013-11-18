# Begins workers on ec2 instances
# Usage : run.sh word_count 20
# word_count is the name of the executable, 20 is the number of nodes to run on


ALLNODES=`ec2-describe-instances  -F instance-state-code=16 | cut  -f17`
ALLPVTNODES=`ec2-describe-instances  -F instance-state-code=16 | cut  -f18`
rm config

# Constructs the configuration file

example=$1 
numNodes=$2

j=0;
for i in $ALLPVTNODES;
do
    if [ $j -lt $numNodes ]; then  
        echo $j $i:54673 >> config;
        j=$[j+1]; 
    fi
done

j=0;
for i in $ALLNODES; 
do 
    if [ $j -lt $numNodes ]; then  
	    scp -o 'StrictHostKeyChecking no' config ubuntu@$i:/home/ubuntu/GoMapReduce/bin
	    if [ $j -ne 0 ]; then
		ssh -o 'StrictHostKeyChecking no' ubuntu@$i \
	       "nohup /home/ubuntu/GoMapReduce/bin/$example -rank=$j -config=/home/ubuntu/GoMapReduce/bin/config -inputdir=/home/ubuntu/input/ &" &
	    fi
	    j=$[j+1]; 
    fi
done; 

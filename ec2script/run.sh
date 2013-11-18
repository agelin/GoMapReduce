ALLNODES=`ec2-describe-instances  -F instance-state-code=16 | cut  -f17`
ALLPVTNODES=`ec2-describe-instances  -F instance-state-code=16 | cut  -f18`
rm config
#Make config

example=$1 
numNodes=$2

j=0;
for i in $ALLPVTNODES;
do
    echo $j $i:54673 >> config;
    j=$[j+1]; 
done

j=0;
for i in $ALLNODES; 
do 
    if [ $j -lt $numNodes ]; then  
	    scp -o 'StrictHostKeyChecking no' config ubuntu@$i:/home/ubuntu/GoMapReduce/bin/
	    if [ $j -ne 0 ]; then
		ssh -o 'StrictHostKeyChecking no' ubuntu@$i \
	       "nohup /home/ubuntu/GoMapReduce/bin/$example -rank=$j -config=/home/ubuntu/GoMapReduce/bin/config -inputdir=/home/ubuntu/input/ &" &
	    fi
	    j=$[j+1]; 
    fi
done; 

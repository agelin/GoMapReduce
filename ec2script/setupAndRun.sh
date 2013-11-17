
ALLNODES=`ec2-describe-instances  -F instance-state-code=16 | cut  -f17`
ALLPVTNODES=`ec2-describe-instances  -F instance-state-code=16 | cut  -f18`
rm config
#Make config
j=0;
for i in $ALLPVTNODES;
do
    echo $j $i:54673 >> config;
    j=$[j+1]; 
done

j=0;
for i in $ALLNODES; 
do 
    echo $i; 
    scp -o 'StrictHostKeyChecking no' aec2_script.sh ubuntu@$i:/home/ubuntu;
    scp -o 'StrictHostKeyChecking no' config ubuntu@$i:/home/ubuntu/GoMapReduce/bin/
    ssh -o 'StrictHostKeyChecking no' ubuntu@$i \
		'echo $i; \
		/sbin/ifconfig; \
        chmod 711 aec2_script.sh; \
		sh /home/ubuntu/aec2_script.sh' 
    if [ $j -ne 0 ]; then
    ssh -o 'StrictHostKeyChecking no' ubuntu@$i \
        "nohup /home/ubuntu/GoMapReduce/bin/word_count -rank=$j -config=/home/ubuntu/GoMapReduce/bin/config -inputdir=/home/ubuntu/input/ &"
    fi
    j=$[j+1]; 
done; 

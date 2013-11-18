ALLNODES=`ec2-describe-instances  -F instance-state-code=16 | cut  -f17`
for i in $ALLNODES; 
do 
    echo $i; 
    scp -o 'StrictHostKeyChecking no' aec2_script.sh ubuntu@$i:/home/ubuntu;
    ssh -o 'StrictHostKeyChecking no' ubuntu@$i \
		'echo $i; \
		/sbin/ifconfig; \
        chmod 711 aec2_script.sh; \
		sh /home/ubuntu/aec2_script.sh' 
    scp -o 'StrictHostKeyChecking no' config ubuntu@$i:/home/ubuntu/GoMapReduce/bin/
done; 

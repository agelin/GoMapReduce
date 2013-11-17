
ALLNODES=`ec2-describe-instances  -F instance-state-code=16 | cut  -f17`

for i in $ALLNODES;
do

    ssh -o 'StrictHostKeyChecking no' ubuntu@$i \
        "pkill word_count" ;
done

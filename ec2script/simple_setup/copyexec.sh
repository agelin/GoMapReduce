# Copies executables to all nodes
# Must be run from within the bin directory where the executables live


ALLNODES=`ec2-describe-instances  -F instance-state-code=16 | cut  -f17`

for i in $ALLNODES; 
do 
    echo $i; 
    scp -o 'StrictHostKeyChecking no' config ubuntu@$i:/home/ubuntu/
    scp -o 'StrictHostKeyChecking no' word_count ubuntu@$i:/home/ubuntu/
    scp -o 'StrictHostKeyChecking no' anagrams ubuntu@$i:/home/ubuntu/
    scp -o 'StrictHostKeyChecking no' average_word_count ubuntu@$i:/home/ubuntu/
    scp -o 'StrictHostKeyChecking no' inverted_index ubuntu@$i:/home/ubuntu/
    scp -o 'StrictHostKeyChecking no' max_temperature ubuntu@$i:/home/ubuntu/
    scp -o 'StrictHostKeyChecking no' lang_dictionary ubuntu@$i:/home/ubuntu/

done; 

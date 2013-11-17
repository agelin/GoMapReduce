#!/bin/bash
j=0; 
for i in `ec2-describe-instances  -F instance-state-code=16 | cut  -f18`
    do 
        echo $j $i:54673; 
        j=$[j+1]; 
    done

#!/bin/bash

CURDIR=$PWD
SRC=src/examples/$1
SRC_GO=$SRC/"$1.go"
INDIR=$SRC/"$1Inp"  
OUTDIR_GO=$SRC/Output/Go
OUTDIR_HADOOP=$SRC/Output/Hadoop
RESDIR=$CURDIR/Results
RESFILE=$RESDIR/"FinalResult$1"
CLASSNAME="${2%%.*}"

FILES="LargeLong
Large
MedLong
Med
Small"


if [ -n "$1" ]
then

    if [ -f $RESFILE ]
	then		
	    rm -f $RESFILE
    fi

    for i in $FILES 
    do
	runidx=0
	if [ -d $INDIR/$i ]
	then

	    echo "Starting processing $i ..."
	    echo $i  "Size=" `du -h $INDIR/$i`>>$RESFILE
	    
	    while [ $runidx -lt 5 ]
	    do
		if [ -d $OUTDIR_GO/$i ]
		then
			rm -rf $OUTDIR_GO/$i
		fi
		
		mkdir $OUTDIR_GO/$i
		
		echo "Run $(($runidx+1)) ..."
		go run $SRC_GO $INDIR/$i  $OUTDIR_GO/$i
		
		echo "Run $runidx: " >> $RESFILE 
		cat  $OUTDIR_GO/$i/ResponseTime >> $RESFILE
		echo "" >> $RESFILE
		runidx=$[$runidx+1]
	    done
	    
	    echo "" >> $RESFILE 
	    echo "Completed processing $i ..."
	else
		echo "Input directory $INDIR/$i doesn't exist, skipping task .."	
	fi
    done
else
    echo "Argument Missing: test1.sh <GoFolderName>  'e.g test1.sh Anagrams' "
fi




#for i in $FILES 
#do
#	echo "hadoop jar $2 $CLASSNAME $INDIR/$i $OUTDIR_HADOOP/$i"
#done

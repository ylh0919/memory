#!/bin/sh
path=`dirname $0`
echo $1 $2 >> ~/tmp.log
echo `date` >> ~/tmp.log
if [ $# -lt 2 ];
then
echo "error: wrong parameters num"
exit
elif [ $1 -eq 0 -o $1 -eq '0' ];
	then
	shift
	echo "restart kungfu $*"
	expect $path/start_kungfu.sh $* >> ~/kungfu_log.txt &
	echo "this is end"
elif [ $1 -eq 1 ];
	then
	shift
	for strategy in $*
	do 	
		flage="false"
		echo "$path"
		while read par1 par2 par3 par4 par5
			do
			  echo "$par1:$strategy"
			  if [ $par1 = $strategy ]
			  then
			    echo "retart strategy:$strategy"
			    expect $path/start_strategy.sh $par1 $par2 $par3 $par4 $par5 >> ~/strategy_log.txt &	
			    flage="true"
			    break
			  fi
			done < $path/strategy_input.txt
		if [ $flage == "false" ]
		then
		echo "don't find $strategy config"
		fi
	done
	echo "this is end"
elif [ $1 -eq 2 ];
	then
	shift
	for pid in $*
	do
		echo kill strategy pid: $pid at: `date "+%Y-%m-%d %H:%M:%S"`
		kill -9 $pid
	done
	echo "this is end"
else
	echo "error: restart wrong process type"
	exit
fi

#!/bin/sh

#config
#before using this scripts, replace scriptPath in kungfu.json with this file's path
# if need_restart_strategy == 1 && function_name == td, then restart python strategy when td* is restart by monitor_daemon 
need_restart_strategy=1 
function_name=td

path=`dirname $0`
echo $1 $2 >> ~/tmp.log

if [ $# -lt 2 ];
then
echo "error: wrong parameters num"
exit
elif [ $1 -eq 0 -o $1 -eq '0' ];
	then
	# stop strategy if need_restart_strategy == 1 and $2 == $function_name*
	if [[ $2 == $function_name* && $need_restart_strategy -eq 1 || $need_restart_strategy -eq '1' ]]; then
		shift
		nohup bash $path/start_python_strategy.sh $* >/dev/null 2>&1 &
	else 
	# restart td
		shift
		echo "restart kungfu $*" >> ~/tmp.log
		expect $path/start_kungfu.sh $* >> ~/kungfu_log.txt &
	fi

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
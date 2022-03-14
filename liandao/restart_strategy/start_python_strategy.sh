#!/bin/sh

#config
sleep_sec_after_stop_strat=30
sleep_sec_before_start_strat=90
# if strategy_name is empty, then kill all strategy when restart strategy
# else kill the strategy name or file name contain strategy_name
strategy_name=
# remember check the scripts with which you restart python strategy
start_python_strategy_path='/opt/kungfu/master/restart_strategy/start_strategy_hedge.py'


path=`dirname $0`
echo $1 >> ~/tmp2.log
echo 'path'$path >> ~/tmp2.log
date >> ~/tmp2.log


# stop strategy
echo "need restart strategy all strategy, stop strategy before "$1" restarts" >> ~/tmp2.log
if [ -z "$strategy_name" ]; then
	echo "strategy_name is empty, kill all strategy" >> ~/tmp2.log
	ps -aux | grep "strategy" | grep -v 'grep' | grep -v "restart" >> ~/tmp2.log
	res=$(ps -aux | grep "strategy" | grep -v 'grep' | grep -v "restart" | awk -F ' '  '{print $2}')
elif [ -n "$strategy_name" ]; then
	echo "strategy_name is not empty, kill "$strategy_name >> ~/tmp2.log
	ps -aux | grep "strategy" | grep $strategy_name | grep -v 'grep' | grep -v "restart" >> ~/tmp2.log
	res=$(ps -aux | grep "strategy" | grep $strategy_name  | grep -v 'grep' | grep -v "restart" | awk -F ' '  '{print $2}')
fi

echo "strategy pid: "$res >> ~/tmp2.log
for pid in $res
do
	echo "kill -9 "$pid >> ~/tmp2.log
	kill -9 $pid
done

#check
ps -aux | grep "strategy" | grep -v 'grep' | grep -v $0 >> ~/tmp2.log
echo "sleep "$sleep_sec_after_stop_strat" after stopping strategy" >> ~/tmp2.log
sleep $sleep_sec_after_stop_strat
	
# restart td
echo "restart kungfu $*" >> ~/tmp2.log
expect $path/start_kungfu.sh $* >> ~/kungfu_log.txt &

# start strategy
echo "sleep "$sleep_sec_before_start_strat" before starting strategy" >> ~/tmp2.log
sleep $sleep_sec_before_start_strat
echo "need restart strategy all strategy, start strategy after "$1" restarts" >> ~/tmp2.log
nohup python -u $start_python_strategy_path >/dev/null 2>~/tmp2.log &

echo "this is end" >> ~/tmp2.log
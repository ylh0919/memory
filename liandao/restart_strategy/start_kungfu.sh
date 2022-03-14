#!/usr/bin/expect
spawn kungfuctl
sleep 5
expect "kungfu>" { send "restart $argv  \n\r" }
sleep 5
set timeout 50
expect "kungfu>" {send "exit \n\r"}
expect eof
exit


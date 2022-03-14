#!/usr/bin/expect
set par0 [lindex $argv 0]
set par1 [lindex $argv 1]
set par2 [lindex $argv 2]
set par3 [lindex $argv 3]
set par4 [lindex $argv 4]
set timeout 10000000000000
spawn /auto/fgl/md_strat/build/manual_strategy $par0 $par1 $par2
sleep 5
send "\r"
sleep 1
expect ">>" { send "8 1 $par3 $par4  \r" }
sleep 5
expect eof

#!/bin/bash
make
clear
cd tests
PID="`ps -ef|grep test_|grep -v 'grep'|awk '{print $2}' ORS=","`"
echo $PID | awk '{split($0,arr,",");cmd="kill -9 "; for(i in arr) system(cmd arr[i])}'
find test_* -type f -executable -exec ./repeat.sh 1 ./local.sh 1 1   ./{} \;

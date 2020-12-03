#!/bin/bash

if [ $# -lt 2 ]; then
 echo "format: ./test.sh [test name: 2A/2B/2C] [test time]"
 echo "parameter should be 2, respersent test name and test times(*4)"
exit 1
fi

testName=$1
testTimes=$2
statusLogName="test_status.log"

function StartTest()
{
 failedlog="failed$1.log"
 for ((i=1; i<=$testTimes; i++))
 do
  x=`go test -race -run $testName 2>&1`
  result=$(echo "$x" | grep "PASS")
  if [ -z "$result" ]
  then
   echo "$x" >$failedlog
   echo "[$1] Failed! After $i times test, find this failed case!!!" >>$statusLogName
   return
  else
   #echo "$x" >$failedlog
   echo "[$1] test $i ok!" >>$statusLogName
  fi
 done
 echo "[$1] Succeed! After $testTimes times test!" >>$statusLogName
}

# clear log
> $statusLogName

StartTest 1 &
StartTest 2 &
StartTest 3 &
StartTest 4 &

wait

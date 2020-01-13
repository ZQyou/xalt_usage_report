#!/bin/bash

mlen=('xx' '31' '28' '31' '30' '31' '30' '31' '31' '30' '31' '30' '31')

if [ $# -lt 5 ]; then
  echo "Usage: $0 year month system <pbs|xalt> <stdout|syslog>"
  exit -1
fi

year=$1
month=$2
system=$3
database=$4
logger=$5

start_t=`printf "%d-%02d-01" $year $month`
end_t=`printf "%d-%02d-%d" $year $month ${mlen[$month]}`

if [ "$database" = "pbs" ]; then
  echo ./pbsacct_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger
  ./pbsacct_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger
fi

if [ "$database" = "xalt" ]; then
  echo ./xalt_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger
  ./xalt_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger
fi

#!/bin/bash

mlen=('xx' '31' '28' '31' '30' '31' '30' '31' '31' '30' '31' '30' '31')

if [ $# -lt 4 ]; then
  echo "Usage: $0 year month system <stdout|syslog>"
  exit -1
fi

year=$1
month=$2
system=$3
logger=$4

start_t=`printf "%d-%02d-01" $year $month`
end_t=`printf "%d-%02d-%d" $year $month ${mlen[$month]}`

echo ./pbsacct_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger
./pbsacct_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger

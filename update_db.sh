#!/bin/bash

mlen=('xx' '31' '28' '31' '30' '31' '30' '31' '31' '30' '31' '30' '31')

if [ $# -lt 4 ]; then
  echo "Usage: $0 year month system <pbs|xalt>"
  exit -1
fi

year=$1
month=$2
system=$3
database=$4
logger=stdout

fh=`printf "%d%02d" $year $month`
start_t=`printf "%d-%02d-01" $year $month`
end_t=`printf "%d-%02d-%d" $year $month ${mlen[$month]}`
f_month=`printf "%02d" $month`

if [ "$database" = "pbs" ]; then
  echo ./pbsacct_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger
  ./pbsacct_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger |grep ^syshost |tee usage/pbs/${fh}${system}.log
fi

if [ "$database" = "xalt" ]; then
  echo ./xalt_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger
  time \
  (./xalt_usage_report.py --start $start_t --end $end_t --num 50 --syshost $system --log $logger |grep ^syshost |while read x; do echo $x year=$year month=$f_month; done) |\
    tee usage/xalt/${fh}${system}.log
fi

#!/bin/bash

if [ "x$startdate" = "x" ] || [ "x$enddate" = "x" ]; then
  echo "Must provide startdate and enddate"
  exit -1
fi

logger=stdout
year=$(date --date="$startdate" +"%Y")
month=$(date --date="$startdate" +"%m")
usage_path="/fs/ess/PZS0710/database/xalt/usage"
syshost_list=${syshost_list:-$LMOD_SYSTEM_NAME}

for syshost in ${syshost_list}
do
  echo "== ${syshost^} update =="
  logfile="${usage_path}/xalt/${year}${month}${syshost}.log"
  tmpfile="${logfile}.tmp"
  ../xalt_usage_report --start $startdate --end $enddate --syshost ${syshost} --db xalt_${syshost} --num 50 --log $logger |& tee $tmpfile
  grep ^syshost $tmpfile |while read x; do echo $x year=$year month=$month; done |tee $logfile
  test -f $tmpfile && rm -f $tmpfile
done

echo "== Push the result to Splunk =="
cd $usage_path
this_year=$(date +"%Y")
last_year=$(date --date="last year" +"%Y")
ls */${this_year}*.log |while read x; do logger -t sw_usage -f $x; done
ls */${last_year}*.log |while read x; do logger -t sw_usage -f $x; done

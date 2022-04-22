#!/bin/bash

enddate=${enddate:-$(date --date="1 days ago" +"%Y-%m-%d")}
startdate=${startdate:-$(date --date="$enddate 14 days ago" +"%Y-%m-%d")}

#echo "== Ascend update =="
#../xalt_usage_report --start $startdate --end $enddate --syshost ascend --db xalt_ascend --topq

echo "== Pitzer update =="
../xalt_usage_report --start $startdate --end $enddate --syshost pitzer --db xalt_pitzer --topq

#echo "== Owens update =="
#../xalt_usage_report --start $startdate --end $enddate --syshost owens --db xalt_owens --topq


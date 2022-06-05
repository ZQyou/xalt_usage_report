#!/bin/bash

enddate=${enddate:-$(date --date="1 days ago" +"%Y-%m-%d")}
days=${days:-"14"}
startdate=${startdate:-$(date --date="$enddate ${days} days ago" +"%Y-%m-%d")}
syshost=${syshost:-$LMOD_SYSTEM_NAME}
pq_path=${pq_path:-"default"}

echo "== ${syshost^} update =="
../xalt_usage_report --start $startdate --end $enddate --syshost ${syshost} --db xalt_${syshost} --topq --pq_path $pq_path

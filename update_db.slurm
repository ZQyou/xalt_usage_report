#!/bin/bash

startdate=$(date --date='15 days ago' +"%Y-%m-%d")
enddate=$(date --date='1 days ago' +"%Y-%m-%d")

echo "== Pitzer update =="
./xalt_usage_report --start $startdate --end $enddate --syshost pitzer --topq

echo "== Owens update =="
./xalt_usage_report --start $startdate --end $enddate --syshost owens --topq


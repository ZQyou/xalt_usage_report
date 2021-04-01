#!/bin/bash


[ "x$enddate" = "x" ] && enddate=$(date --date="1 days ago" +"%Y-%m-%d")
[ "x$startdate" = "x" ] && startdate=$(date --date="$enddate 14 days ago" +"%Y-%m-%d")

echo "== Pitzer update =="
../xalt_usage_report --start $startdate --end $enddate --syshost pitzer --topq

echo "== Owens update =="
../xalt_usage_report --start $startdate --end $enddate --syshost owens --topq


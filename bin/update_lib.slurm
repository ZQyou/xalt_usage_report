#!/bin/bash

# xalt_lib stores cross-searh from xalt_object, join_run_object and xalt_run

echo "== ${syshost^} update =="
../xalt_usage_report --start $startdate --end $enddate --syshost $syshost --library --topq


#!/bin/bash

today=$(date --date='today' +"%Y%m%d")

update_usage() {
  startdate=$1
  enddate=$2
  echo Update software usage from $startdate to $enddate
  sbatch --account=PZS0710 --nodes=1 --exclusive \
         --time=60 \
         --job-name="xalt-usage-update-$today" \
         --output=/fs/ess/PZS0710/database/xalt/logs/update-usage-${startdate}-${enddate}@${today}-%j.log \
         --mail-user=zyou@osc.edu \
         --mail-type=FAIL \
         --export=startdate=${startdate},enddate=${enddate} \
         update_usage.slurm
}

year=$1
month=$2

if [ "x$year" = "x" ] || [ "x$month" = "x" ]; then
  l_startdate=$(date --date="last month" +"%Y-%m-01")
  l_enddate=$(date --date="$l_startdate + 1 month -1 day" +"%Y-%m-%d")
  c_startdate=$(date --date="1 days ago" +"%Y-%m-01")
  [ "$c_startdate" = "$l_startdate" ] && c_startdate=""
  c_enddate=$(date --date="1 days ago" +"%Y-%m-%d")
  # No usage update for previous month after day 15
  [ $(date +"%d") -gt 15 ] && l_startdate=""
else
  c_startdate=$(date --date="$year-$month-01" +"%Y-%m-%d")
  c_enddate=$(date --date="$c_startdate + 1 month -1 day" +"%Y-%m-%d")
fi

if [ -n "$l_startdate" ] && [ -n "$l_enddate" ]; then
  update_usage $l_startdate $l_enddate
fi

if [ -n "$c_startdate" ] && [ -n "$c_enddate" ]; then
  update_usage $c_startdate $c_enddate
fi

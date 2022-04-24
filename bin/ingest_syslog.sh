#!/bin/bash

syslog_prefix=/fs/ess/PZS0710/xalt/logs/2022/$LMOD_SYSTEM_NAME
xalt_mysql_home=/fs/ess/PZS0710/xalt/mysql

syslog_fn=$1
syslog_fn=${syslog_fn:-${LMOD_SYSTEM_NAME}_$(date --date="1 days ago" +"%Y%m%d").log}
syslog_file="$syslog_prefix/$syslog_fn"

if [ ! -f "$syslog_file" ]; then
  echo Cannot find $syslog_file
  exit -1
fi

cd $xalt_mysql_home
echo ./run_syslog_to_db $syslog_file
./run_syslog_to_db $syslog_file

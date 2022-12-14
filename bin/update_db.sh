#!/bin/bash
: << "USAGE"
# Update xalt_run database for all clusers from 14 days before yesterday to yesterday
update_db.sh

# Update xalt_run database for pitzer from 14 days before 2022-06-15 to 2022-06-15
update_db.sh pitzer 2022-06-15

# Update xalt_run database for pitzer from 2022-06-01 to 2022-06-15
update_db.sh pitzer 2022-06-15 2022-06-01

USAGE

syshost=$1
enddate=$2
startdate=$3

enddate=${enddate:-$(date --date="1 days ago" +"%Y-%m-%d")}
startdate=${startdate:-$(date --date="$enddate 14 days ago" +"%Y-%m-%d")}
lib=${lib:-}
today=$(date --date='today' +"%Y%m%d")

update_run() {
  local _startdate=$1
  local _enddate=$2
  local _syshost=$3
  echo Update ${_syshost^} xalt_run from ${_startdate} to ${_enddate}
  sbatch --account=PZS0710 --ntasks=4 \
         --time=59 \
         --job-name="xalt-run-update-$today" \
         --output=/fs/ess/PZS0710/database/xalt/logs/update-run-${_syshost}-${_startdate}-${_enddate}@${today}-%j.log \
         --mail-user=zyou@osc.edu \
         --mail-type=FAIL \
         --export=startdate=${_startdate},enddate=${_enddate},syshost=${_syshost} \
         update_run.slurm
}

update_lib() {
  local _startdate=$1
  local _enddate=$2
  local _syshost=$3
  echo Update ${_syshost^} xalt_lib from ${_startdate} to ${_enddate}
  sbatch --account=PZS0710 --ntasks=8 \
         --time=60 \
         --job-name="xalt-lib-update-$today" \
         --output=/fs/ess/PZS0710/database/xalt/logs/update-lib-${_syshost}-${_startdate}-${_enddate}@${today}-%j.log \
         --mail-user=zyou@osc.edu \
         --mail-type=FAIL \
         --export=startdate=${_startdate},enddate=${_enddate},syshost=${_syshost} \
         update_lib.slurm
}

script_home="`dirname $(readlink -f $0)`"
cd $script_home 

if [ -n "$syshost" ]; then
  if [ "$syshost" = "owens" ] || [ "$syshost" = "pitzer" ] || [ "$syshost" = "ascend" ]; then
    update_run $startdate $enddate $syshost
  else
    echo "Cannot update XALT database for syshost=$syshost"
    exit -1
  fi
else
  update_run $startdate $enddate ascend
  update_run $startdate $enddate pitzer
  update_run $startdate $enddate owens
fi

if [ "$lib" = "owens" ] || [ "$lib" = "pitzer" ]; then
  update_lib $startdate $enddate $lib
fi

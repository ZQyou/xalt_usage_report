#!/bin/bash

enddate=${enddate:-$(date --date="1 days ago" +"%Y-%m-%d")}
startdate=${startdate:-$(date --date="$enddate 14 days ago" +"%Y-%m-%d")}
lib=${lib:-}
today=$(date --date='today' +"%Y%m%d")

update_run() {
  startdate=$1
  enddate=$2
  syshost=$3
  echo Update ${syshost^} xalt_run from $startdate to $enddate
  sbatch --account=PZS0710 --ntasks=4 \
         --time=59 \
         --job-name="xalt-run-update-$today" \
         --output=/fs/ess/PZS0710/database/xalt/logs/update-run-${syshost}-${startdate}-${enddate}@${today}-%j.log \
         --mail-user=zyou@osc.edu \
         --mail-type=FAIL \
         --export=startdate=${startdate},enddate=${enddate},syshost=${syshost} \
         update_run.slurm
}

update_lib() {
  startdate=$1
  enddate=$2
  syshost=$3
  echo Update ${syshost^} xalt_lib from $startdate to $enddate
  sbatch --account=PZS0710 --ntasks=8 \
         --time=60 \
         --job-name="xalt-lib-update-$today" \
         --output=/fs/ess/PZS0710/database/xalt/logs/update-lib-${syshost}-${startdate}-${enddate}@${today}-%j.log \
         --mail-user=zyou@osc.edu \
         --mail-type=FAIL \
         --export=startdate=${startdate},enddate=${enddate},syshost=${syshost} \
         update_lib.slurm
}

script_home="`dirname $(readlink -f $0)`"
cd $script_home 

if [ "$lib" = "owens" ] || [ "$lib" = "pitzer" ]; then
  update_lib $startdate $enddate $lib
else
  update_run $startdate $enddate pitzer
  update_run $startdate $enddate owens
fi

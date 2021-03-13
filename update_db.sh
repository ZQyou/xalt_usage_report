#!/bin/bash

today=$(date --date='today' +"%Y%m%d")
script_home="`dirname $(readlink -f $0)`"

cd $script_home 
sbatch --account=PZS0710 --ntasks=4 \
       --job-name="xalt-db-update-$today" \
       --output=/fs/ess/PZS0710/database/xalt/logs/update-${today}-%j.log \
       --mail-user=zyou@osc.edu \
       --mail-type=FAIL \
       update_db.slurm


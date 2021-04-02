# Install
## Setup virtual environment
These software usage tools requires Python 3 modules that not available on system. To avoid package conflict with the system, create isolated Python environment:
```
module load python/3.6-conda5.2
mkdir -p /usr/local/software_usage/venv/$LMOD_SYSTEM_NAME
python -m venv --without-pip /usr/local/reframe/vevn/$LMOD_SYSTEM_NAME
source /usr/local/venv/software_usage/$LMOD_SYSTEM_NAME/bin/activate
($LMOD_SYSTEM_NAME) > curl https://bootstrap.pypa.io/get-pip.py |python
($LMOD_SYSTEM_NAME) > pip install -r requirements.txt
($LMOD_SYSTEM_NAME) > deactivate
```

# Update Software Usage
## Daily update local xalt_run
A cronjob is scheduled for every 10pm to update local xalt_run (`/fs/project/PZS0710/database/xalt`) for last 14 days:
```
00 22 * * *  $HOME/software_usage/update_db.sh
```
An output example:
```
Update xalt_run from 2021-02-26 to 2021-03-12
Submitted batch job 3378244
```
## Daily update software usage
A cronjob is scheduled for every 11pm to update [software usage](https://splunk.osc.edu/en-US/app/osc/sciapps_software_usage) for previous and current months:
```
00 23 * * *  $HOME/software_usage/update_usage.sh
```
An outuput example:
```
Update software usage from 2021-02-01 to 2021-02-28
Submitted batch job 3386140
Update software usage from 2021-03-01 to 2021-03-13
Submitted batch job 3386141
```

# XALT
## Description 
`xalt_usage_report` analyzes XATL database and generates a usage report of software/executables or modules By default the script generates a week-to-date software usage report for the system where you login. Use following options to filter or change the output.

## Note for output
* `CPUHrs`: walltime x # cores x # threads, NOT actual CPU utilization.

## Command-line options
* `--sw`: print software/exectuable usage (default)
* `--module`: print module usage
* `--execrun`: print executable path; this is break-down of `--sw` mode
* `--sql`: SQL pattern for matching software/executable/execpath/module, depending on the report option (`%` is SQL wildcard character)
* `--days`: report from now to DAYS back
* `--start`: start date, e.g. 2018-12-25
* `--end`: end date
* `--num`: top number of entries to report (default is 20)
* `--sort`: sort the result by corehours (default) | users | jobs | date | n_cores | n_thds 
* `--username`: print username instead of # users
* `--group`: print username and primary group
* `--account`: print job accounts
* `--gpu`: print usage for GPU jobs
* `--mpi`: print usage for parallel jobs
* `--user`: user name for matching
* `--jobs`: print job ids and dates
* `--csv`: print in CSV format

## Use cases
Get lammps usage within one week
```
xalt_usage_report --sql lammps
```
Get the module usage sorted by \# jobs from now to 2 days back
```
xalt_usage_report --module --sorted jobs --days 2
```
Find what users using lammps
```
xalt_usage_report --module --sql %lammps% --username
```

# PBSACCT
## Description 
`pbsacct_usage_report` analyzes PBSACCT database and generates a software usage report. By default the script generates a week-to-date report for the cluster where you login. Use following options to filter or change the output.

## Note for output
* `CPUHrs`: walltime x # procs
* `NodeHrs`: walltime x # nodes
* `Efficiency`: cpu_t / (walltime x # procs)

## Command-line options
* `--sql`: SQL pattern for matching software. (`%` is SQL wildcard character)
* `--start`: start date, e.g. 2018-12-25
* `--end`: end date
* `--syshost`: search by syshost (default is $LMOD_SYSTEM_NAME)
* `--num`: top number of entries to report (default is 20)
* `--sort`: sort the result by corehours (default) | nodehours | users | groups | accounts | jobs | software 
* `--username`: print user names, accounts and groups instead of # users, # accounts and # groups
* `--user`: user name for matching
* `--host`: search by hostname
* `--queue`: search by queue: serial | parallel | lognserial | longparallel | largeparallel | largemem | hugemem
* `--rsvn`: search by reservation: gpu | pfs | ime
* `--jobs`: print job ids and dates
* `--days`: report from now to DAYS back
* `--csv`: print in CSV format


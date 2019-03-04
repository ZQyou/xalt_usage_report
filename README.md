# XALT
## Description 
`xalt_usage_report.py` analyzes XATL database and generates a usage report of modules or executables. By default the script generates a week-to-date report for both module and executable usages for the cluster where you login. Use following options to filter or change the output.

## Note for output
* `CoreHrs`: walltime x # cores x # threads, NOT actual CPU utilization.

## Command-line options
* `--module`: print module usage only
* `--execrun`: print executable usage only
* `--sql`: SQL pattern for matching modules or executables. (`%` is SQL wildcard character)
* `--start`: start date, e.g. 2018-12-25
* `--end`: end date
* `--num`: top number of entries to report (default is 20)
* `--sort`: sort the result by corehours (default) | users | jobs | date | n_cores | n_thds 
* `--username`: print user names instead of # users
* `--group`: print user names and groups
* `--gpu`: print GPU usage 
* `--user`: user name for matching
* `--jobs`: print job ids and dates
* `--days`: report from now to DAYS back
* `--csv`: print in CSV format

# PBSACCT
## Description 
`pbsacct_usage_report.py` analyzes PBSACCT database and generates a software usage report. By default the script generates a week-to-date report for the cluster where you login. Use following options to filter or change the output.

## Note for output
* `CoreHrs`: cput_sec
* `NodeHrs`: walltime x # nodes

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
* `--queue`: search by queue: serial | lognserial | parallel | longparallel | largeparallel | hugemem
* `--rsvn`: search by reservation: gpu | pfs | ime
* `--jobs`: print job ids and dates
* `--days`: report from now to DAYS back
* `--csv`: print in CSV format

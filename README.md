# XALT
## Description 
`xalt_usage_report.py` analyzes XATL database and generates a usage report of software/executables or modules By default the script generates a week-to-date software usage report for the system where you login. Use following options to filter or change the output.

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
* `--username`: print user names instead of # users
* `--group`: print user names and groups
* `--gpu`: print GPU usage 
* `--user`: user name for matching
* `--jobs`: print job ids and dates
* `--csv`: print in CSV format

## Use cases
Get lammps usage within one week
```
xalt_usage_report.py --sql lammps
```
Get the module usage sorted by \# jobs from now to 2 days back
```
xalt_usage_report.py --module --sorted jobs --days 2
```
Find what users using lammps
```
xalt_usage_report.py --module --sql %lammps% --username
```

# PBSACCT
## Description 
`pbsacct_usage_report.py` analyzes PBSACCT database and generates a software usage report. By default the script generates a week-to-date report for the cluster where you login. Use following options to filter or change the output.

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


# README
## Description 
`xalt_usage_report.py` analyzes XATL database and generates a usage report in terms of modules or executables. 

### Note
By default the script generates a weekly report for both module and executable usages. 

### Command-line options
* `--module`: print module usage only
* `--executable`: print executable usage only
* `--sql`: SQL pattern for matching modules or executables. (`%` is SQL wildcard character)
* `--start`: start date, e.g. 2018-12-25
* `--end`: end date
* `--num`: top number of entries to report
* `--sort`: sort the result by corehours (default) | users | jobs | date
* `--username`: print user accounts instead of # users
* `--group`: print user accounts and groups
* `--gpu`: print GPU usage 
* `--user`: user account for matching
* `--jobs`: print job ids and dates

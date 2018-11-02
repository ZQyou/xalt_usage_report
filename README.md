# README
## Description

## Usage
Generate a full report
```
./xalt_usage_report.py
```
Generate a full report with more detail
```
./xalt_usage_report.py --full
``` 
Report the usage of a particular module 
```
./xalt_usage_report.py --module --sql 'mvapich2/2.3%'
```
Report the usage of a particular module with usernames
```
./xalt_usage_report.py --username --module --sql 'mvapich2/2.3%'
```
Report the usage of a particular executable
```
./xalt_usage_report.py --execrun --sql '%pmemd%'
```
Report top 50 modules used by a particular user starting from October, 2018:
```
./xalt_usage_report.py --module --user 'sciappstest' --num 50 --start 2018-10-01
```

## To-do
* Current usage report for executables is limited to those executables with modules. Add option to disable or enable it.
* Fix incorrect username report 

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
./xalt_usage_report.py --module mvapich2
```
Report the usage of a particular executable
```
./xalt_usage_report.py --execrun pmemd
```
Report top 50 modules used by a particular user starting from October, 2018:
```
./xalt_usage_report.py --user sciappstest --num 50 --start 2018-10-01
```

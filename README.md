# README
## Description

## Usage
Generate a full report
```
./xalt_usage_report.py
```
Report the module usage sorted by # users
```
./xalt_usage_report.py --module --sort n_users
```
Report the usage of a particular module 
```
./xalt_usage_report.py --module --sql 'mvapich2/2.3%'
```
Report the usage of a particular module with usernames
```
./xalt_usage_report.py --module --sql 'mvapich2/2.3%' --username
```
Report the executable usage
```
./xalt_usage_report.py --execrun
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

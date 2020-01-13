# Upload montly PBS and XALT software usage 
```
ls */*.log |while read x; do logger -t sw_usage -f $x; done
```

#!/usr/bin/env python3

from subprocess import Popen, PIPE
from re import search, compile, IGNORECASE
from time import strftime
from datetime import datetime, timedelta
from calendar import monthrange
#from guppy import hpy

def get_heap_status():
  print()
  # https://coderzcolumn.com/tutorials/python/guppy-heapy-profile-memory-usage-in-python
  #heap = hpy()
  #heap_status = heap.heap()
  #print("Heap Size : ", heap_status.size, " bytes\n")
  #print(heap_status)

group_re = compile('Primary Group:\s+(.*)\n', IGNORECASE)
def get_osc_group(username):
  process = Popen(['OSCfinger %s' % username], shell=True, stdout=PIPE, stderr=PIPE)
  stdout, stderr = process.communicate()
  return search(group_re, stdout.decode('utf-8')).group(1)

def pbs_set_time_range(startD, endD, days=7):
  enddate = strftime("%Y-%m-%d")
  if endD is not None:
    enddate = endD
  
  # Generate week-to-date report by default
  startdate = (datetime.strptime(enddate, "%Y-%m-%d") - timedelta(days-1)).strftime("%Y-%m-%d")
  if startD is not None:
    startdate = startD

  isotimefmt = "%Y-%m-%dT%H:%M:%S"
  startdate_t = startdate + "T00:00:00"
  enddate_t = enddate + "T23:59:59"
  startdate_t = datetime.strptime(startdate_t, isotimefmt).strftime("%s")
  enddate_t = datetime.strptime(enddate_t, isotimefmt).strftime("%s")

  return [startdate, enddate, startdate_t, enddate_t]

def xalt_set_time_range(startD, endD, days=7, mode='weekly'):
  enddate = strftime('%Y-%m-%d')
  if endD is not None:
    enddate = endD
  # Generate weekly report by default
  startdate = (datetime.strptime(enddate, "%Y-%m-%d") - timedelta(days-1)).strftime('%Y-%m-%d');
  if startD is not None:
    startdate = startD
  # Evaluate time range
  days = (datetime.strptime(enddate, "%Y-%m-%d") - datetime.strptime(startdate, "%Y-%m-%d")).days + 1
  res, mod, delta = [0, days, 0 ] if mode == 'daily' else [days%7, days/7, 6]
  startdate_t = []
  enddate_t = []
  s = startdate
  for n in range(int(mod)):
    e = (datetime.strptime(s, "%Y-%m-%d") + timedelta(delta)).strftime('%Y-%m-%d');
    s_p = datetime.strptime(s, "%Y-%m-%d")
    e_p = datetime.strptime(e, "%Y-%m-%d")
    if s_p.month != e_p.month:
      e1 = '%04d-%02d-%02d' % (s_p.year, s_p.month, monthrange(s_p.year, s_p.month)[1])
      startdate_t.append(s +  ' 00:00:00')
      enddate_t.append(e1 + ' 23:59:59')
      s = '%04d-%02d-%02d' % (e_p.year, e_p.month, 1)
    startdate_t.append(s +  ' 00:00:00')
    enddate_t.append(e + ' 23:59:59')
    s = (datetime.strptime(e, "%Y-%m-%d") + timedelta(1)).strftime('%Y-%m-%d');
  if res > 0:
    e = (datetime.strptime(s, "%Y-%m-%d") + timedelta(res-1)).strftime('%Y-%m-%d');
    startdate_t.append(s +  ' 00:00:00')
    enddate_t.append(e + ' 23:59:59')
  
  return [startdate, enddate, startdate_t, enddate_t]

if __name__ == '__main__': 
  startD = '2019-09-01'
  endD = '2019-11-30'
  #startdate, enddate, startdate_t, enddate_t = xalt_set_time_range(startD, endD)
  startdate, enddate, startdate_t, enddate_t = xalt_set_time_range(startD, endD, mode='daily')
  print(startdate, enddate)
  print(len(startdate_t))
  print(startdate_t)
  print(enddate_t)


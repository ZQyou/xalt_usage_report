#!/usr/bin/env python3
from subprocess import Popen, PIPE
from re import search, compile, IGNORECASE
from time import strftime, time
from datetime import date, datetime, timedelta
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
def get_user_group(username):
  cmd = 'OSCfinger %s' % username
  process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
  stdout, stderr = process.communicate()
  return search(group_re, stdout.decode('utf-8')).group(1)

def get_job_account(jobs, N=2000):
  from operator import methodcaller
  import numpy as np
  import sys
  accounts = []
  t0 = time()
  u, indices = np.unique(jobs, return_inverse=True)
  print("Time for finding unique jobs: %.2fs" % (float(time() - t0)))
  print('Converting %d unique jobs to accounts' % u.size)
  t0 = time()
  for i in range(0, u.size, N):
    chk = u[i:i+N]
    cmd = "sacct -X -o Account,JobIDRaw -P -j %s" % (','.join(chk))
#
# sacct -X -j  2739714,2889587,2891573,2891574, -o jobidraw,jobid,account
#    JobIDRaw        JobID    Account 
#    ------------ ------------ ---------- 
#    2739714      2739714         pls0146 
#    2891573      2889587_1       pas1200 
#    2891574      2889587_2       pas1200 
#    2889587      2889587_3       pas1200 
#
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    lines = stdout.decode('utf-8').split('\n')[1:-1]
    acct_jid = np.array(list(map(methodcaller("split", "|"), lines)))
    acct = acct_jid[:,0]
    jid  = acct_jid[:,1]
    p = [ acct[np.where(jid == j)] for j in chk ]
    if len(p) != len(chk):
      print('Unmatched number of accounts (%d) with the number of jobs (%d:%d)' % (len(p), i, i+N))
      print(chk)
      print(p)
      print(cmd)
      sys.exit(1)

    accounts += p

  print("Time for getting project numbers: %.2fs" % (float(time() - t0)))
  return np.asarray(accounts)[indices]

def xalt_set_time_range(startD, endD, days=7, mode='weekly'):
  enddate = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
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


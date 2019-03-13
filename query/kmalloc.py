import csv, numpy
import sys, os
from pbsacct import Software
from BeautifulTbl import BeautifulTbl
from datetime import datetime, timedelta
from .conf import set_timerange

class kmalloc(object):
  def __init__(self, kmalloc_file):
    timestamps = []
    hosts = []
    with open(kmalloc_file, mode='r') as infile:
      reader = csv.reader(infile)
      headers = next(reader, None)
      i_time = headers.index('_time')
      i_host = headers.index('host')
      for row in reader:
        timestamps.append(row[i_time])
        hosts.append(row[i_host])
      infile.close()

    timestamps.reverse()
    hosts.reverse()
    #print(timestamps)
    self.u_hosts = numpy.unique(hosts)
    self.u_times = []
    for h in self.u_hosts:
        self.u_times.append(timestamps[hosts.index(h)])

    print(self.u_hosts)
    print(self.u_times)

  def print_by_host(self, cursor, args):
    isotimefmt = "%Y-%m-%dT%H:%M:%S"
    for i, t in enumerate(self.u_times):
      enddate = t.split('T')[0]
      enddate_t = t.split('.')[0]
      startdate, enddate, startdate_t, enddate_t = set_timerange(args, enddate, enddate_t)
  
      args.host = self.u_hosts[i]
      if args.host[0] == 'p':
        args.syshost = 'pitzer'
      elif args.host[0] == 'o':
        args.syshost = 'owens'
      elif args.host[0] == 'r':
        args.syshost = 'ruby'
      else:
        sys.exit(1)
      queryA = Software(cursor)
      queryA.build(args, startdate_t, enddate_t)
      headerA, resultA, statA = queryA.report_by(args)
  
      if resultA:
        print("--------------------------------------------")
        print("PBSACCT QUERY from",startdate,"to",enddate)
        print("--------------------------------------------")
        print(headerA)
        bt = BeautifulTbl(tbl=resultA, gap = 2)
        print(bt.build_tbl());
        print()


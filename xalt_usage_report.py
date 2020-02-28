#!/usr/bin/env python3

import os, sys

dirNm = os.environ.get("OSC_XALT_DIR","./")
sys.path.insert(1,os.path.realpath(os.path.join(dirNm, "libexec")))
sys.path.insert(1,os.path.realpath(os.path.join(dirNm, "site")))

from sql import CmdLineOptions, Sql
from log import syslog_logging
from query import *

def main():
  ##### Setup #####
  args = CmdLineOptions().execute()
  sql = Sql(args)
  sql.connect()
  startdate, enddate, startdate_t, enddate_t = xalt_set_time_range(
          args.startD, args.endD, int(args.days))

  resultA = None
  queryA = None
  headerA = None
  statA = None

  #### Update Local Databae ####
  if args.topq:
    queryA = Xalt(sql.conn)
    queryA.to_parquet(args, startdate_t, enddate_t)
    sys.exit(0)

  #### DEBUG ####
  if args.show:
    if args.show != "tables":
      resultA = sql.describe_table()
      headerA = "\nDescribe the table '%s'\n" % args.show
    else:
      resultA = sql.show_tables()
      headerA = "\nAvailable tables in database\n"
  if args.query:
    resultA = sql.user_query()
    if not resultA:
        sys.exit(0)

  #### Syslog ####
  if args.log:
    args.sw = True
    args.module = args.execpath = False
    queryA = Xalt(sql.conn)
    queryA.build(args, startdate_t, enddate_t)
    resultA = queryA.report_by(args)
    print("--------------------------------------------")
    print("XALT Software Usage from",startdate,"to",enddate)
    print("--------------------------------------------")
    syslog_logging(args.syshost, 'xalt', resultA, args.log)
    sys.exit(0)
  
  #### Software Usage ####
  args.username = True if args.group else args.username
  args.sw = False if args.module or args.execpath or args.library else args.sw 
  if not resultA:
    queryA = Xalt(sql.conn)
    queryA.build(args, startdate_t, enddate_t)
    headerA, resultA, statA = queryA.report_by(args)

# if not resultA and args.library:
#   queryA = Library(sql.conn)
#   queryA.build(args, startdate_t, enddate_t)
#   headerA, resultA, statA = queryA.report_by(args)

  if resultA and args.csv:
    print("XALT Software Usage from",startdate,"to",enddate)
    print(",".join(resultA[0]))
    for row in resultA[2:]:
      print(",".join(row))
    sys.exit(0)

  if resultA:
    print("-------------------------------------------------")
    print("XALT Software Usage from",startdate,"to",enddate)
    print("-------------------------------------------------")
    print(headerA)
    bt = BeautifulTbl(tbl=resultA, gap = 2)
    print(bt.build_tbl());
    print()

  if statA:
    num_unlist = statA['num'] - int(args.num)
    if num_unlist > 0:
      print("Unlisted entries: %d" % num_unlist)
    print("Total entries: %d" % statA['num'])
    if 'jobs' in statA:
      print("Total jobs: %d" % statA['jobs'])
    if 'cpuhours' in statA:
      print("Total cpuhours: %.2f" % statA['cpuhours'])
    print()

if ( __name__ == '__main__'): main()

#!/usr/bin/env python2

from __future__ import print_function
import os, sys, base64
import MySQLdb, argparse
import time
from datetime import datetime, timedelta
from pprint import pprint
from query import *
from util import *

syshost = os.environ.get("LMOD_SYSTEM_NAME", "%")

class CmdLineOptions(object):
  """ Command line Options class """

  def __init__(self):
    """ Empty Ctor """
    pass
  
  def execute(self):
    """ Specify command line arguments and parse the command line"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--confFn",  dest='confFn',    action="store",       default = "pbsacctdb.cfg",help="db name")
    parser.add_argument("--start",   dest='startD',    action="store",       default = None,           help="start date, e.g 2018-10-23")
    parser.add_argument("--end",     dest='endD',      action="store",       default = None,           help="end date")
    parser.add_argument("--syshost", dest='syshost',   action="store",       default = syshost,        help="syshost")
    parser.add_argument("--num",     dest='num',       action="store",       default = 20,             help="top number of entries to report")
    parser.add_argument("--sql",     dest='sql',       action="store",       default = "%",            help="SQL pattern for matching software; '%%' is SQL wildcard character")
    parser.add_argument("--user",    dest='user',      action="store",       default = None,           help="search by user account")
    parser.add_argument("--host",    dest='host',      action="store",       default = None,           help="search by hostname")
    parser.add_argument("--queue",   dest='queue',     action="store",       default = None,           help="search by queue: serial | longserial | parallel | longparallel | hugemem")
    parser.add_argument("--sort",    dest='sort',      action="store",       default = None,           help="sort by corehours (default) | users | jobs | date")
    parser.add_argument("--username",dest='username',  action="store_true",                            help="print username instead of n_users")
    parser.add_argument("--gpu",     dest='gpu',       action="store_true",                            help="report the usage with num_gpus > 0")
    parser.add_argument("--jobs",    dest='jobs',      action="store_true",                            help="list executables by date")
    parser.add_argument("--csv",     dest='csv',       action="store_true",                            help="print in CSV format")
    parser.add_argument("--jobid",   dest='jobid',     action="store",       default = None,           help="search by jobid")
    parser.add_argument("--dbg",     dest='dbg',       action="store",       default = None,           help="full sql command (DEBUG)")
    parser.add_argument("--show",    dest='show',      action="store",       default = None,           help="show/describe tables of thea database, e.g. --show tables")
    parser.add_argument("--data",    dest='data',      action="store",       default = None,           help="list data by given columns")
    parser.add_argument("--report",  dest='report',    action="store_true",                            help="report from original xalt_usage_report.py")
    parser.add_argument("--full",    dest='full',      action="store_true",                            help="report core hours by compiler")
    parser.add_argument("--kmalloc", dest='kmalloc',   action="store",       default = None,           help="read splunk csv and report usage for kmalloc events")
    parser.add_argument("--days",    dest='days',      action="store",       default = 7,              help="report from now to DAYS back")
    args = parser.parse_args()
    return args


def main():
  ##### Configuration #####
  args    = CmdLineOptions().execute()
  config  = pbsacct_conf(syshost, args.confFn)
  conn = MySQLdb.connect        \
         (config.get("pbsacct","HOST"), \
          config.get("pbsacct","USER"), \
          base64.b64decode(config.get("pbsacct","PASSWD")), \
          config.get("pbsacct","DB"))
  cursor = conn.cursor()

  enddate = time.strftime("%Y-%m-%d")
  if args.endD is not None:
    enddate = args.endD
  
  # Generate weekly report by default
  startdate = (datetime.strptime(enddate, "%Y-%m-%d") - timedelta(int(args.days))).strftime("%Y-%m-%d")
  if args.startD is not None:
    startdate = args.startD

  startdate_t = startdate + "T00:00:00"
  enddate_t = enddate + "T23:59:59"
  isotimefmt = "%Y-%m-%dT%H:%M:%S"
  startdate_t = datetime.strptime(startdate_t, isotimefmt).strftime("%s")
  enddate_t = datetime.strptime(enddate_t, isotimefmt).strftime("%s")

  headerA = None
  resultA = None

  #### Kmalloc ####
  if args.kmalloc:
    sys.exit(0)

  #### Debug ####
  if args.show:
    if args.show != "tables":
      resultA = describe_table(cursor, args)
      headerA = "\nDescribe the table '%s'\n" % args.show
    else:
      resultA = show_tables(cursor)
      headerA = "\nAvailable tables in database\n"
  if args.data:
    resultA = pbsacct_select_jobs(cursor, args, startdate_t, enddate_t)
  if args.dbg:
    resultA = user_sql(cursor, args)

  #### Software Usage ####
  if args.jobid:
    queryA = Job(cursor)
    queryA.build(args)
    queryA.report_by()

  if not resultA:
    queryA = Software(cursor)
    queryA.build(args, startdate_t, enddate_t)
    headerA, resultA, statA = queryA.report_by(args)
  
  if resultA and args.csv:
    print("PBSACCT QUERY from",startdate,"to",enddate)
    print(",".join(resultA[0]))
    for row in resultA[2:]:
      print(",".join(row))
    sys.exit(0)

  if resultA:
    print("--------------------------------------------")
    print("PBSACCT QUERY from",startdate,"to",enddate)
    print("--------------------------------------------")
    print(headerA)
    bt = BeautifulTbl(tbl=resultA, gap = 2)
    print(bt.build_tbl());
    print()

if ( __name__ == '__main__'): main()

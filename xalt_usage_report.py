#!/usr/bin/env python2

from __future__ import print_function
import os, sys, base64
import MySQLdb, argparse
import time
from datetime import datetime, timedelta
from pprint import pprint
try:
  import configparser
except:
  import ConfigParser as configparser

dirNm = os.environ.get("OSC_XALT_DIR","./")
sys.path.insert(1,os.path.realpath(os.path.join(dirNm, "libexec")))
sys.path.insert(1,os.path.realpath(os.path.join(dirNm, "site")))

from query import *
from report import *

class CmdLineOptions(object):
  """ Command line Options class """

  def __init__(self):
    """ Empty Ctor """
    pass
  
  def execute(self):
    """ Specify command line arguments and parse the command line"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--confFn",  dest='confFn',    action="store",       default = "xalt_db.conf", help="db name")
    parser.add_argument("--start",   dest='startD',    action="store",       default = None,           help="start date, e.g 2018-10-23")
    parser.add_argument("--end",     dest='endD',      action="store",       default = None,           help="end date")
    parser.add_argument("--syshost", dest='syshost',   action="store",       default = "%",            help="syshost")
    parser.add_argument("--module",  dest='module',    action="store_true",                            help="print module usage only")
    parser.add_argument("--execrun", dest='execrun',   action="store_true",                            help="printt executable usage only")
    parser.add_argument("--sql",     dest='sql',       action="store",       default = "%",            help="SQL pattern for matching modules or executables; '%%' is SQL wildcard character")
    parser.add_argument("--num",     dest='num',       action="store",       default = 20,             help="top number of entries to report")
    parser.add_argument("--sort",    dest='sort',      action="store",       default = None,           help="sort the result by corehours (default) | users | jobs | date")
    parser.add_argument("--username",dest='username',  action="store_true",                            help="print user accounts instead of # users")
    parser.add_argument("--group",   dest='group',     action="store_true",                            help="print user accounts and groups")
    parser.add_argument("--gpu",     dest='gpu',       action="store_true",                            help="print GPU usage")
    parser.add_argument("--user",    dest='user',      action="store",       default = None,           help="user account for matching")
    parser.add_argument("--jobs",    dest='jobs',      action="store_true",                            help="print job ids and dates")
    parser.add_argument("--csv",     dest='csv',       action="store_true",                            help="print in CSV format")
    parser.add_argument("--dbg",     dest='dbg',       action="store",       default = None,           help="full SQL command (DEBUG)")
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
  XALT_ETC_DIR = os.environ.get("XALT_ETC_DIR","./")
  args         = CmdLineOptions().execute()
  config       = configparser.ConfigParser()     
  configFn     = os.path.join(XALT_ETC_DIR,args.confFn)
  config.read(configFn)

  conn = MySQLdb.connect              \
         (config.get("MYSQL","HOST"), \
          config.get("MYSQL","USER"), \
          base64.b64decode(config.get("MYSQL","PASSWD")), \
          config.get("MYSQL","DB"))
  cursor = conn.cursor()

  enddate = time.strftime('%Y-%m-%d')
  if (args.endD is not None):
    enddate = args.endD
  
  # Generate weekly report by default
  startdate = (datetime.strptime(enddate, "%Y-%m-%d") - timedelta(int(args.days))).strftime('%Y-%m-%d');
  if (args.startD is not None):
    startdate = args.startD

  startdate_t = startdate + ' 00:00:00'
  enddate_t = enddate + ' 23:59:59'


  ##### Kmalloc ####
  if args.kmalloc:
    import csv
    timestamps = []
    hosts = []
    with open(args.kmalloc, mode='r') as infile:
      reader = csv.reader(infile)
      headers = next(reader, None)
      i_time = headers.index('_time')
      i_host = headers.index('host')
      for row in reader:
        timestamps.append(row[i_time])
        hosts.append(row[i_host])
      infile.close()

    print(timestamps)
    print(hosts)

    sys.exit(0)


  ##### Report (original XALT usage report from XALT package) #####
  if args.report:
    print("--------------------------------------------")
    print("XALT REPORT from",startdate,"to",enddate)
    print("--------------------------------------------")
    print("")
    print("")
    full_report(cursor, args, startdate_t, enddate_t)
    sys.exit(0)

  ##### Query #####
  resultA = None
  resultB = None
  queryA = None
  queryB = None
  headerA = None
  headerB = None
  statA = None
  statB = None

  args.username = True if args.group else args.username

  # debug
  if args.show:
    if args.show != "tables":
      resultA = describe_table(cursor, args)
      headerA = "\nDescribe the table '%s'\n" % args.show
    else:
      resultA = show_tables(cursor)
      headerA = "\nAvailable tables in database\n"
  if args.data:
    resultA = xalt_select_data(cursor, args, startdate_t, enddate_t)
  if args.dbg:
    resultA = user_sql(cursor, args)
  
  # search by username
  if args.execrun:
    queryA = ExecRun(cursor)
    queryA.build(args, startdate_t, enddate_t)
    headerA, resultA, statA = queryA.report_by(args)
  
  if args.module:
    queryA = Module(cursor)
    queryA.build(args, startdate_t, enddate_t)
    headerA, resultA, statA = queryA.report_by(args)

  if not resultA:
    queryA = Module(cursor)
    queryB = ExecRun(cursor)
    if queryA:
      queryA.build(args, startdate_t, enddate_t)
      headerA, resultA, statA = queryA.report_by(args)
    if queryB:
      queryB.build(args, startdate_t, enddate_t)
      headerB, resultB, statB = queryB.report_by(args)

  if resultA and args.csv:
    print("XALT QUERY from",startdate,"to",enddate)
    print(",".join(resultA[0]))
    for row in resultA[2:]:
      print(",".join(row))
    sys.exit(0)

  if resultA:
    print("--------------------------------------------")
    print("XALT QUERY from",startdate,"to",enddate)
    print("--------------------------------------------")
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
    if 'corehours' in statA:
      print("Total corehours: %.2f" % statA['corehours'])
    print()

  if resultB:
    print("--------------------------------------------")
    print("XALT QUERY from",startdate,"to",enddate)
    print("--------------------------------------------")
    print(headerB)
    bt = BeautifulTbl(tbl=resultB, gap = 2)
    print(bt.build_tbl());
    print()

  if statB:
    num_unlist = statB['num'] - int(args.num)
    if num_unlist > 0:
      print("Unlisted entries: %d" % num_unlist)
    print("Total entries: %d" % statB['num'])
    if 'jobs' in statB:
      print("Total jobs: %d" % statB['jobs'])
    if 'corehours' in statB:
      print("Total corehours: %.2f" % statB['corehours'])
    print()

if ( __name__ == '__main__'): main()

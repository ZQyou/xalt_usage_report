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

from BeautifulTbl      import BeautifulTbl

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
    parser.add_argument("--num",     dest='num',       action="store",       default = 20,             help="top number of entries to report")
    parser.add_argument("--module",  dest='module',    action="store_true",                            help="report module usage")
    parser.add_argument("--execrun", dest='execrun',   action="store_true",                            help="report executable usage")
    parser.add_argument("--sql",     dest='sql',       action="store",       default = "%",            help="sql search pattern used with --module and --execrun; use '%%' as wildcard")
    parser.add_argument("--user",    dest='user',      action="store",       default = None,           help="search by user account")
    parser.add_argument("--sort",    dest='sort',      action="store",       default = "corehours",    help="sort by corehours (default) | n_users | n_jobs")
    parser.add_argument("--username",dest='username',  action="store_true",                            help="print username instead of n_users")
    parser.add_argument("--gpu",     dest='gpu',       action="store_true",                            help="report the usage with num_gpus > 0")
    parser.add_argument("--group",   dest='group',     action="store_true",                            help="print username and group")
    parser.add_argument("--jobs",    dest='jobs',      action="store_true",                            help="list executables by date")
    parser.add_argument("--dbg",     dest='dbg',       action="store",       default = None,           help="full sql command (DEBUG)")
    parser.add_argument("--list",    dest='list',      action="store",       default = None,           help="show/describe tables")
    parser.add_argument("--data",    dest='data',      action="store",       default = None,           help="list data by given columns")
    parser.add_argument("--report",  dest='report',    action="store_true",                            help="report from original xalt_usage_report.py")
    parser.add_argument("--full",    dest='full',      action="store_true",                            help="report core hours by compiler")
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
  
  startdate = (datetime.strptime(enddate, "%Y-%m-%d") - timedelta(90)).strftime('%Y-%m-%d');
  if (args.startD is not None):
    startdate = args.startD

  startdate_t = startdate + ' 00:00:00'
  enddate_t = enddate + ' 23:59:59'

  ##### Report #####
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

  # debug
  if args.list:
    if args.list != "tables":
      resultA = describe_table(cursor, args)
      headerA = "\nDescribe the table '%s'\n" % args.list
    else:
      resultA = show_tables(cursor)
      headerA = "\nAvailable tables in database\n"
  if args.data:
    resultA = select_data(cursor, args, startdate_t, enddate_t)
  if args.dbg:
    resultA = user_sql(cursor, args)
  
  # search by module/executable sql pattern
# if args.sql == None:
#   if args.module:
#     queryA = ModuleExec(cursor)
#     headerA = "\nTop %s modules sorted by %s\n" % (str(args.num), args.sort)
#     queryA.build(args, startdate_t, enddate_t)
#     resultA = queryA.report_by(args, args.sort)
#   if args.execrun:
#     queryA = ExecRun(cursor)
#     headerA = "\nTop %s executables sorted by %s\n" % (str(args.num), args.sort)
#     queryA.build(args, startdate_t, enddate_t)
#     resultA, sumCH = queryA.report_by(args, args.sort)
# else:
  
  # search by username
  if args.user:
    if args.execrun:
      if args.jobs:
        queryA = ExecRunListbyUser(cursor)
        headerA = "\nFirst %s executables used by %s\n" % (str(args.num), args.user)
      else:
        queryA = ExecRunCountbyUser(cursor)
        headerA = "\nTop %s executables used by %s\n" % (str(args.num), args.user)
    else:
      queryA = ModuleCountbyUser(cursor)
      headerA = "\nTop %s modules used by %s\n" % (str(args.num), args.user)
    queryA.build(args, startdate_t, enddate_t)
    if args.jobs:
      resultA, statA = queryA.report_by(args)
    else:
      resultA = queryA.report_by(args)

  if not resultA:
    args.username = True if args.group else args.username
    if args.module:
      if args.username: 
        queryA = UserCountbyModule(cursor)
        headerA = "\nTop %s '%s' modules used by users\n" % (str(args.num), args.sql)
      else: 
        queryA = ModuleCountbyName(cursor)
        headerA = "\nTop %s '%s' modules sorted by %s\n" % (str(args.num), args.sql, args.sort)
    if args.execrun:
      if args.username:
        queryA = UserCountbyExecRun(cursor)
        headerA = "\nTop %s '%s' excutables by users\n" % (str(args.num), args.sql)
      elif args.jobs:
        queryA = ExecRunListbyName(cursor)
        headerA = "\nFirst %s '%s' excutables sorted by date\n" % (str(args.num), args.sql)
      else:
        queryA = ExecRunCountbyName(cursor)
        headerA = "\nTop %s '%s' executables sorted by %s\n" % (str(args.num), args.sql, args.sort)

    if not queryA:
      queryA = ModuleCountbyName(cursor)
      headerA = "\nTop %s '%s' modules sorted by %s\n" % (str(args.num), args.sql, args.sort)
      queryB = ExecRunCountbyName(cursor)
      headerB = "\nTop %s '%s' executables sorted by %s\n" % (str(args.num), args.sql, args.sort)

    if queryA:
      queryA.build(args, startdate_t, enddate_t)
      if args.jobs:
        resultA, statA = queryA.report_by(args)
      else:
        resultA = queryA.report_by(args)

    if queryB:
      queryB.build(args, startdate_t, enddate_t)
      resultB = queryA.report_by(args)

  if resultA:
    print("--------------------------------------------")
    print("XALT QUERY from",startdate,"to",enddate)
    print("--------------------------------------------")
    print("")
    print(headerA)
    bt = BeautifulTbl(tbl=resultA, gap = 2)
    print(bt.build_tbl());
    print()

  if statA:
    print("Number of entries: %d" % statA['num'])
    print()

  if resultB:
    print("")
    print(headerB)
    bt = BeautifulTbl(tbl=resultB, gap = 2)
    print(bt.build_tbl());
    print()

if ( __name__ == '__main__'): main()

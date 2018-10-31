from __future__ import print_function
import os, sys, base64
import MySQLdb, argparse
import time
from operator import itemgetter
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
    parser.add_argument("--data",    dest='data',      action="store",       default = None,           help="list data by given columns")
    parser.add_argument("--sql",     dest='sql',       action="store",       default = None,           help="user sql")
    parser.add_argument("--module",  dest='module',    action="store",       default = None,           help="module count")
    parser.add_argument("--execrun", dest='execrun',   action="store",       default = None,           help="executable count")
    parser.add_argument("--user",    dest='user',      action="store",       default = None,           help="module and exec used by user")
    parser.add_argument("--sort",    dest='sort',      action="store",       default = "corehours",    help="sort by corehours*/n_users/n_jobs")
    parser.add_argument("--list",    dest='list',      action="store_true",                            help="describe xalt_run tables")
    parser.add_argument("--full",    dest='full',      action="store_true",                            help="report core hours by compiler (DEFAULT)")
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

  ##### Query #####
  print("--------------------------------------------")
  print("XALT QUERY from",startdate,"to",enddate)
  print("--------------------------------------------")
  print("")
  resultA = None
  if args.list:
    resultA = describe_xalt_run(cursor)
    print("\nDescribe XALT table xalt_run\n") 
  if args.data:
    resultA = list_data(cursor, args, startdate, enddate)
  if args.sql:
    resultA = user_sql(cursor, args)
  if args.module:
    if args.module == "report":
      modA = ModuleExec(cursor)
      modA.build(args, startdate, enddate)
      resultA = modA.report_by(args, args.sort)
      print("\nTop %s modules sorted by %s\n" % (str(args.num), args.sort))
    else:
      modA = ModuleCountbyName(cursor)
      modA.build(args, startdate, enddate)
      resultA = modA.report_by(args)
      print("\nTop %s '%s' modules sorted by %s\n" % (str(args.num), args.module, args.sort))
  if args.execrun:
    modA = ExecRunCountbyName(cursor)
    modA.build(args, startdate, enddate)
    resultA = modA.report_by(args)
    print("\nTop %s '%s' executables sorted by %s\n" % (str(args.num), args.execrun, args.sort))
  if args.user:
    #modA = ModuleCountbyUser(cursor)
    modA = ExecRunCountbyUser(cursor)
    modA.build(args, startdate, enddate)
    resultA = modA.report_by(args)
    #print("\nTop %s modules used by %s\n" % (str(args.num), args.user))
    print("\nTop %s executables used by %s\n" % (str(args.num), args.user))

  if resultA:
    bt = BeautifulTbl(tbl=resultA, gap = 2)
    print(bt.build_tbl());
    print()
    sys.exit(0)

  ##### Report #####
  print("--------------------------------------------")
  print("XALT REPORT from",startdate,"to",enddate)
  print("--------------------------------------------")
  print("")
  print("")
  full_report(cursor, args, startdate, enddate)

if ( __name__ == '__main__'): main()

#!/usr/bin/env python3

import os, sys
import pymysql, argparse
from sql import Sql
from log import syslog_logging
from query import *

syshost = os.environ.get("LMOD_SYSTEM_NAME", "%")

class CmdLineOptions(object):
  """ Command line Options class """

  def __init__(self):
    """ Empty Ctor """
    pass
  
  def execute(self):
    """ Specify command line arguments and parse the command line"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",  dest='confFn',    action="store",       default = None,           help="db name")
    parser.add_argument("--start",   dest='startD',    action="store",       default = None,           help="start date, e.g 2018-10-23")
    parser.add_argument("--end",     dest='endD',      action="store",       default = None,           help="end date")
    parser.add_argument("--days",    dest='days',      action="store",       default = 7,              help="report from now to DAYS back")
    parser.add_argument("--syshost", dest='syshost',   action="store",       default = syshost,        help="set syshost (default is $LMOD_SYSTEM_NAME)")
    parser.add_argument("--num",     dest='num',       action="store",       default = 20,             help="top number of entries to report (default is 20)")
    parser.add_argument("--sql",     dest='sql',       action="store",       default = "%",            help="SQL pattern for matching software/jobname; '%%' is SQL wildcard character")
    parser.add_argument("--jobname", dest='jobname',   action="store_true",                            help="search by jobname instead of sw_app")
    parser.add_argument("--rev",     dest='rev',       action="store_true",                            help="enabe reverse search for software/jobname")
    parser.add_argument("--user",    dest='user',      action="store",       default = None,           help="filtered by user account")
    parser.add_argument("--host",    dest='host',      action="store",       default = None,           help="filtered by hostname")
    parser.add_argument("--queue",   dest='queue',     action="store",       default = None,           help="filtered by queue: serial | longserial | parallel | longparallel | hugemem")
    parser.add_argument("--rsvn",    dest='rsvn',      action="store",       default = None,           help="filtered by reservation: gpu | pfs | ime")
    parser.add_argument("--sort",    dest='sort',      action="store",       default = None,           help="sort by cpuhours (default) | nodehours | mem | users | jobs | date")
    parser.add_argument("--username",dest='username',  action="store_true",                            help="print username instead of n_users")
    parser.add_argument("--jobs",    dest='jobs',      action="store_true",                            help="list jobs by date")
    parser.add_argument("--nodelist",dest='nodelist',  action="store_true",                            help="print nodelist in job list mode (--jobs)")
    parser.add_argument("--jobid",   dest='jobid',     action="store",       default = None,           help="search by jobid")
    parser.add_argument("--dbg",     dest='dbg',       action="store",       default = None,           help="full sql command (DEBUG)")
    parser.add_argument("--show",    dest='show',      action="store",       default = None,           help="show/describe tables of thea database, e.g. --show tables")
    parser.add_argument("--data",    dest='data',      action="store",       default = None,           help="list data by given columns")
#   parser.add_argument("--kmalloc", dest='kmalloc',   action="store",       default = None,           help="read splunk csv and report usage for kmalloc events")
#   parser.add_argument("--gmetric", dest='gmetric',   action="store",       default = None,           help="ganglia metric in JSON, e.g. mem_s_unreclaimable")
#   parser.add_argument("--webpass", dest='webpass',   action="store",       default = None,           help="password for ganglia access. prompt is default")
#   parser.add_argument("--webuser", dest='webuser',   action="store",       default = None,           help="password for ganglia access. $USER is default")
    parser.add_argument("--csv",     dest='csv',       action="store_true",                            help="print in CSV format")
    parser.add_argument("--log",     dest='log',       action="store",       default = None,           help="dump the result to log: stdout | syslog")
    args = parser.parse_args()
    return args


def main():
  ##### Configuration #####
  args = CmdLineOptions().execute()
  sql  = Sql(args, db='pbsacct')
  sql.connect()
  cursor = sql.cursor
  startdate, enddate, startdate_t, enddate_t = pbs_set_time_range(
          args.startD, args.endD, int(args.days))

  headerA = None
  resultA = None
  statA = None

  if args.log:
    queryA = Software(cursor)
    queryA.build(args, startdate_t, enddate_t)
    resultA = queryA.report_by(args)
    print("--------------------------------------------")
    print("PBSACCT Software Usage from",startdate,"to",enddate)
    print("--------------------------------------------")
    syslog_logging(args.syshost, 'pbsacct', resultA, args.log)
    sys.exit(0)

  #### Debug ####
  if args.show:
    if args.show != "tables":
      resultA = describe_table(cursor, args)
      headerA = "\nDescribe the table '%s'\n" % args.show
    else:
      resultA = show_tables(cursor)
      headerA = "\nAvailable tables in database\n"
  if args.dbg:
    resultA = user_sql(cursor, args)
    if not resultA:
        sys.exit(0)

  if args.jobid:
    import getpass, json
    queryA = Job(cursor)
    queryA.build(args)
    resultA = queryA.report_by()
    print(json.dumps(resultA[0], indent=4, sort_keys=True))
    sys.exit(0)

  #### Software Usage ####
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
    print("----------------------------------------------------")
    print("PBSACCT Software Usage from",startdate,"to",enddate)
    print("----------------------------------------------------")
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

if __name__ == '__main__':
  main()

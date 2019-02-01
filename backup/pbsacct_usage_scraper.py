#!/usr/bin/env python2

from __future__ import print_function
import os, sys
from pprint import pprint
import argparse
import scraper

class CmdLineOptions(object):
  def __init__(self):
    pass
  def execute(self):
    """ Specify command line arguments and parse the command line"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--syshost", dest='syshost',   action="store",       required = True,          help="syshost")
    parser.add_argument("--start",   dest='startD',    action="store",       required = True,          help="start date, e.g 2018-10-23")
    parser.add_argument("--end",     dest='endD',      action="store",       required = True,          help="end date")
    parser.add_argument("--num",     dest='num',       action="store",       default = 20,             help="top number of entries to report")
    parser.add_argument("--user",    dest='user',      action="store",       default = None,           help="search by user account")
    parser.add_argument("--sort",    dest='sort',      action="store",       default = "cpuhours",     help="sort by cpuhours (default) | jobs | nodehours | groups | accounts | charges ")
    parser.add_argument("--jobs",    dest='jobs',      action="store_true",  default = False,          help="job information")
    parser.add_argument("--script",  dest='script',    action="store_true",  default = False,          help="print script")
    parser.add_argument("--sw",      dest='sw',        action="store",       default = None,           help="filter by software name")
    args = parser.parse_args()
    return args


def main():
    
  args = CmdLineOptions().execute()

  if args.user: 
    print("User: %s" % args.user)
  print("System: %s" % args.syshost)
  print("Start Date: %s" % args.startD)
  print("End Date: %s" % args.endD)

  result = None
  header = None
  if args.user:
    if args.jobs:  
      result = scraper.jobs_by_user(args)
      header = "\nJobs owend by user %s\n" % (args.user)
    else:
      result = scraper.by_user(args)
      header = "\nAll packages used by user %s\n" % (args.user)
    result.get(args=args)
  else:
    result = scraper.summary(args)
    result.get(9,args=args)
    header = "\nAll packages sorted by %s\n" % (args.sort)

  print("-----------------------------------------------")
  print("PBSACCT REPORT from",args.startD,"to",args.endD)
  print("-----------------------------------------------")
  print("")
  print(header)
  result.print()
  print()
    
if __name__ == '__main__':
  main()

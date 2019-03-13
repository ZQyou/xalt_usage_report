from __future__ import print_function
import time
from datetime import datetime, timedelta
import os
try:
  import configparser
except:
  import ConfigParser as configparser


def pbsacct_conf(syshost, confFn):
  config  = configparser.ConfigParser()     
  if not confFn:
    script_path = os.path.dirname(os.path.realpath(__file__))
    confFn = os.path.join(script_path,"..","conf",".db_conf")
    if not os.path.isfile(confFn):
      if syshost == "pitzer":
        confFn = os.path.join("/apps/software_usage","conf",".db_conf")
      elif syshost == "owens":
        confFn = os.path.join("/usr/local/software_usage","conf",".db_conf")
  #print("Loading the config file", confFn)
  config.read(confFn)

  return config

if __name__ == '__main__': 
  syshost = os.environ.get("LMOD_SYSTEM_NAME", "%")
  config = pbsacct_conf(syshost, None)
  print(config.get("pbsacct","HOST"))

def set_timerange(args, enddate=None, enddate_t=None):
  if not enddate:
    enddate = time.strftime("%Y-%m-%d")
    if args.endD is not None:
      enddate = args.endD
  
  # Generate week-to-date report by default
  startdate = (datetime.strptime(enddate, "%Y-%m-%d") - timedelta(int(args.days))).strftime("%Y-%m-%d")
  if args.startD is not None:
    startdate = args.startD

  isotimefmt = "%Y-%m-%dT%H:%M:%S"
  startdate_t = startdate + "T00:00:00"
  if not enddate_t:
    enddate_t = enddate + "T23:59:59"
  startdate_t = datetime.strptime(startdate_t, isotimefmt).strftime("%s")
  enddate_t = datetime.strptime(enddate_t, isotimefmt).strftime("%s")

  return [startdate, enddate, startdate_t, enddate_t]

from subprocess import Popen, PIPE
from re import search, compile, IGNORECASE
import time
from datetime import datetime, timedelta

group_re = compile('Primary Group:\s+(.*)\n', IGNORECASE)
def get_osc_group(username):
  process = Popen(['OSCfinger %s' % username], shell=True, stdout=PIPE, stderr=PIPE)
  stdout, stderr = process.communicate()
  return search(group_re, stdout).group(1)

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

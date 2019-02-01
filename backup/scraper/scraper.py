from __future__ import print_function
from .base import _base

#https://pbsacct.osc.edu/user-software.php?username=sciappstest&system=pitzer&start_date=2018-12-05&end_date=2018-12-07&datelogic=start&order=cpuhours
class by_user(_base):
  def __init__(self, args):
    url = 'https://pbsacct.osc.edu/user-software.php?username=%s&system=%s&start_date=%s&end_date=%s&datelogic=start&order=%s' % \
            (args.user, args.syshost, args.startD, args.endD, args.sort)
    _base.__init__(self,url,[])

#https://pbsacct.osc.edu/usage-summary.php?system=pitzer&start_date=2018-12-05&end_date=2018-12-07&datelogic=start&software=1&order=cpuhours&table=1
class summary(_base):
  def __init__(self, args):
    url = 'https://pbsacct.osc.edu/usage-summary.php?system=%s&start_date=%s&end_date=%s&datelogic=start&order=%s' % \
            (args.syshost, args.startD, args.endD, args.sort)
    options = ['software','table']
    _base.__init__(self,url,options)

#https://pbsacct.osc.edu/jobs-by-user.php?username=ohu0511&system=owens&start_date=2018-12-20&end_date=2018-12-21&datelogic=start&account=1&jobname=1&nproc=1&nodes=1&cput_sec=1&walltime_sec=1&hostlist=1&exit_status=1&sw_app=1
class jobs_by_user(_base):
  def __init__(self, args):
    url = 'https://pbsacct.osc.edu/jobs-by-user.php?username=%s&system=%s&start_date=%s&end_date=%s&datelogic=start' % \
            (args.user, args.syshost, args.startD, args.endD)
    #options = ['account','jobname','nproc','nodes','queue','start_ts','end_ts','cput_sec','walltime_sec','hostlist','exit_status','sw_app']
    options = ['account','nproc','nodes','queue','start_ts','cput_sec','walltime_sec','exit_status','sw_app']
    if args.script:
      options.append('script')
    _base.__init__(self,url,options)

from operator import itemgetter
from .util import get_osc_group
from datetime import datetime

def SoftwareFormat(args):
  headerA = "\nTop %s software sorted by %s on %s\n" % (str(args.num), args.sort, args.syshost)
  headerT = ["CoreHrs", "NodeHrs", "# Jobs", "# Users", "# Groups", "# Accounts", "Software"]
  fmtT    = ["%.2f", "%.2f", "%d", "%d", "%d", "%d", "%s"]
  orderT  = ['corehours', 'nodehours', 'jobs', 'users', 'groups', 'accounts', 'software']
  if args.username:
    headerA = "\nTop %s software used by users on %s\n" % (str(args.num), args.syshost)
    headerT = ["CoreHrs", "NodeHrs", "# Jobs", "User", "Group", "Account", "Software"]
    fmtT    = ["%.2f", "%.2f", "%d", "%s", "%s", "%s", "%s"]
    orderT  = ['corehours', 'nodehours', 'jobs', 'users', 'groups', 'accounts', 'software']
  if args.user:
    headerA = "\nTop %s executables used by %s on %s\n" % (str(args.num), args.user, args.syshost)
    headerT = ["CoreHrs", "NodeHrs", "# Jobs", "Software"]
    fmtT    = ["%.2f", "%.2f", "%d", "%s"]
    orderT  = ['corehours', 'nodehours', 'jobs', 'software']
  if args.jobs:
    headerA = "\nFirst %s jobs sorted by %s on %s\n" % (str(args.num), args.sort, args.syshost)
    if args.user:
      headerA = "\nFirst %s jobs used by %s on %s\n" % (str(args.num), args.user, args.syshost)
    headerT = ["Start Date", "JobID", "Jobname", "CoreHrs", "NodeHrs", "# CPU", "User", "Group", "Account", "Software"]
    fmtT    = ["%s", "%s", "%s",  "%.2f", "%.2f", "%s", "%s", "%s", "%s", "%s"]
    orderT  = ['date', 'jobs', 'jobname', 'corehours', 'nodehours', 'nproc',  'users', 'groups', 'accounts', 'software']

  headerA += '\n'
  if args.sql != '%':
    headerA += '* Search pattern: %s\n' % args.sql
  if args.host:
    headerA += '* on Host: %s\n' % args.host
  if args.queue:
    headerA += '* on Queue: %s\n' % args.queue

  return [headerA, headerT, fmtT, orderT]

class Software:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    select_runtime = """
    ROUND(SUM(cput_sec)/3600,2)             as corehours,
    ROUND(SUM(walltime_sec*nodect)/3600,2)  as nodehours,
    """
    select_jobs  = "COUNT(DISTINCT(jobid)) as n_jobs, "
    select_user  = """
    COUNT(DISTINCT(username))           as n_users,
    COUNT(DISTINCT(groupname))          as n_groups,
    COUNT(DISTINCT(account))            as n_accounts,
    """
    search_user  = ""
    search_host  = ""
    search_queue = ""
    group_by     = "group by sw_app"

    if args.host:
      search_host = "and hostlist like '%s%s%s' " % ('%%', args.host, '%%')

    if args.queue:
      search_queue = "and queue like '%s' " % (args.queue)

    if args.user or args.username:
      select_user  = "username, groupname, account, "
      if args.user:
        search_user = "and username like '%s' " % args.user
      if args.username:
        group_by = "group by username, groupname, account, sw_app"

    if args.jobs:
      select_runtime = """
      ROUND(cput_sec/3600,2)             as corehours,
      ROUND(walltime_sec*nodect/3600,2)  as nodehours,
      """
      select_user  = "username, groupname, account, "
      select_jobs = "SUBSTRING_INDEX(jobid, \".\", 1), "
      #select_jobs = "jobid, "
      group_by = ""
      args.sort    = 'date' if not args.sort else args.sort

    args.sort = 'corehours' if not args.sort else args.sort

    query = """ SELECT """ + \
    select_runtime + \
    select_jobs + \
    select_user + \
    """
    queue, 
    nproc,
    jobname,
    sw_app as software,
    start_ts
    from Jobs where system like %s
    and sw_app like %s
    """ + \
    search_user + \
    search_host + \
    search_queue + \
    " and start_ts >= %s and start_ts <= %s " % (startdate, enddate) + \
    group_by
    #print(query)

    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.sql))
    resultA = cursor.fetchall()
    modA = self.__modA
    for corehours, nodehours, jobs, users, groups, accounts, queue, nproc, jobname, software, date_ts in resultA:
      entryT = { 'corehours' : corehours,
                 'nodehours' : nodehours,
                 'jobs'      : jobs,
                 'users'     : users,
                 'groups'    : groups,
                 'accounts'  : accounts,
                 'queue'     : queue,
                 'nproc'     : nproc,
                 'software'  : software,
                 'jobname'   : jobname,
                 'date'      : datetime.fromtimestamp(date_ts).strftime("%Y-%m-%d %H:%M:%S")}
      modA.append(entryT)
      ### datetime.utcfromtimestamp(date_ts)

  def report_by(self, args):
    resultA = []
    headerA, headerT, fmtT, orderT = SoftwareFormat(args)
    hline  = map(lambda x: "-"*len(x), headerT)
    resultA.append(headerT)
    resultA.append(hline)

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(map(lambda x, y: x % entryT[y], fmtT, orderT))

    statA = {'num': len(sortA),
             'corehours': sum([x['corehours'] for x in sortA])}
    return [headerA, resultA, statA]

class Job:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args):
    print()
    items = ['username','groupname','account','jobname','nproc','nodes','queue','start_ts','end_ts','cput_sec','walltime_sec','hostlist','exit_status','sw_app']
    query = """ SELECT """ + \
    ",".join(items) + \
    """
    from Jobs where system like %s
    and SUBSTRING_INDEX(jobid, ".", 1) = %s
    """
    #print(query)

    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.jobid))
    resultA = cursor.fetchall()[0]
    modA = self.__modA
    #print(resultA)
    entryT = {}
    for i, key in enumerate(items):
      entryT[key] =  resultA[i]
    #print(entryT)
    modA.append(entryT)

  def report_by(self):
    modA = self.__modA
    print(modA)

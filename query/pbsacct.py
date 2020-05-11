from operator import itemgetter
from .util import get_osc_group
from datetime import datetime
from time import time
#import json

def SoftwareFormat(args):
  headerA = "\nTop %s software sorted by %s on %s\n" % (str(args.num), args.sort, args.syshost)
  headerT = ["CPUHrs", "NodeHrs", "EFF", "Mem (MB)", "# Jobs", "# Users", "# Groups", "# Accounts", "Software"]
  fmtT    = ["%.2f", "%.2f", "%.2f", "%d", "%d", "%d", "%d", "%d", "%s"]
  orderT  = ['cpuhours', 'nodehours', 'efficiency', 'mem', 'jobs', 'users', 'groups', 'accounts', 'software']
  if args.username:
    headerA = "\nTop %s software used by users on %s\n" % (str(args.num), args.syshost)
    headerT = ["CPUHrs", "NodeHrs", "EFF", "Mem (MB)", "# Jobs", "User", "Group", "Account", "Software"]
    fmtT    = ["%.2f", "%.2f",  "%.2f", "%d", "%d", "%s", "%s", "%s", "%s"]
    orderT  = ['cpuhours', 'nodehours', 'efficiency', 'mem', 'jobs', 'users', 'groups', 'accounts', 'software']
  if args.user:
    headerA = "\nTop %s executables used by %s on %s\n" % (str(args.num), args.user, args.syshost)
    headerT = ["CPUHrs", "NodeHrs", "EFF", "Mem (MB)", "# Jobs", "Software"]
    fmtT    = ["%.2f", "%.2f", "%.2f", "%d", "%d", "%s"]
    orderT  = ['cpuhours', 'nodehours', 'efficiency', 'mem', 'jobs', 'software']
  if args.jobs:
    headerA = "\nFirst %s jobs sorted by %s on %s\n" % (str(args.num), args.sort, args.syshost)
    if args.user:
      headerA = "\nFirst %s jobs used by %s on %s\n" % (str(args.num), args.user, args.syshost)
    if args.nodelist:
      headerT = ["Start Date", "JobID", "Jobname", "CPUHrs", "NodeHrs", "EFF", "# CPU", "Mem (MB)", "User", "Group", "Account", "Software", "Nodes"]
      fmtT    = ["%s", "%s", "%s",  "%.2f", "%.2f", "%.2f", "%s", "%d", "%s", "%s", "%s", "%s", "%s"]
      orderT  = ['date', 'jobs', 'jobname', 'cpuhours', 'nodehours', 'efficiency', 'nproc',  'mem', 'users', 'groups', 'accounts', 'software', 'nodelist']
    else:
      headerT = ["Start Date", "JobID", "Jobname", "CPUHrs", "NodeHrs", "EFF", "# CPU", "Mem (MB)", "User", "Group", "Account", "Software"]
      fmtT    = ["%s", "%s", "%s",  "%.2f", "%.2f", "%.2f", "%s", "%d", "%s", "%s", "%s", "%s"]
      orderT  = ['date', 'jobs', 'jobname', 'cpuhours', 'nodehours', 'efficiency', 'nproc',  'mem', 'users', 'groups', 'accounts', 'software']

  headerA += '\n'
  if args.sql != '%':
    headerA += '* Search pattern: %s' % args.sql
    headerA += ' (reverse)\n' if args.rev else '\n'
  if args.host:
    headerA += '* Host: %s\n' % args.host
  if args.queue:
    headerA += '* Queue: %s\n' % args.queue
  if args.rsvn:
    headerA += '* Reservation: %s\n' % args.rsvn

  return [headerA, headerT, fmtT, orderT]

class Software:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    select_runtime = """
    ROUND(SUM(walltime_sec*nproc)/3600,2)   as cpuhours,
    ROUND(SUM(cput_sec)/3600,2)             as corehours,
    ROUND(SUM(walltime_sec*nodect)/3600,2)  as nodehours,
    ROUND(SUM(mem_kb)/1024,0)               as mem,
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
    search_rsvn  = ""
    search_sw = "and LOWER(sw_app) not like %s " if args.rev else "and LOWER(sw_app) like %s "
    search_jobname = ""
    group_by     = "group by sw_app"

    if args.host:
      search_host = "and hostlist like '%s%s%s' " % ('%%', args.host, '%%')

    if args.queue:
      search_queue = "and queue like '%s' " % (args.queue)

    if args.rsvn:
      search_host = "and nodes like '%s%s%s' " % ('%%', args.rsvn, '%%')

    if args.user or args.username:
      select_user  = "username, groupname, account, "
      if args.user:
        search_user = "and username like '%s' " % args.user
      if args.username:
        group_by = "group by username, groupname, account, sw_app"

    if args.jobname:
      #search_jobname = "and jobname like %s "
      search_jobname = "and jobname not like %s " if args.rev else "and jobname like %s "
      search_sw = ""

    if args.jobs:
      select_runtime = """
      ROUND(walltime_sec*nproc/3600,2)   as cpuhours,
      ROUND(cput_sec/3600,2)             as corehours,
      ROUND(walltime_sec*nodect/3600,2)  as nodehours,
      ROUND(mem_kb/1024,0)               as mem,
      """
      select_user  = "username, groupname, account, "
      select_jobs = "SUBSTRING_INDEX(jobid, \".\", 1), "
      #select_jobs = "jobid, "
      group_by = ""
      args.sort    = 'date' if not args.sort else args.sort

    args.sort = 'cpuhours' if not args.sort else args.sort

    query = """ SELECT """ + \
    select_runtime + \
    select_jobs + \
    select_user + \
    """
    queue, nproc, jobname, sw_app, start_ts, hostlist
    from Jobs where system like %s
    """ + \
    search_sw + \
    search_user + \
    search_host + \
    search_jobname + \
    search_queue + \
    search_rsvn + \
    " and start_ts >= %s and start_ts <= %s " % (startdate, enddate) + \
    group_by
    #print(query)

    cursor  = self.__cursor
    print("\nData processing ....")
    print("=============")
    t0 = time()
    cursor.execute(query, (args.syshost, args.sql.lower()))
    resultA = cursor.fetchall()
    print("Query time: %.2fs" % (float(time() - t0)))
    print("=============\n")

    modA = self.__modA
    for cpuhours, corehours, nodehours, mem, jobs, users, groups, accounts, queue, nproc, jobname, software, date_ts, hostlist in resultA:
      efficiency = 0 if cpuhours == 0 else corehours/cpuhours
      entryT = { 'cpuhours'  : cpuhours,
                 'corehours' : corehours,
                 'nodehours' : nodehours,
                 'efficiency': efficiency,
                 'mem'       : mem,
                 'jobs'      : jobs,
                 'users'     : users,
                 'groups'    : groups,
                 'accounts'  : accounts,
                 'queue'     : queue,
                 'nproc'     : nproc,
                 'software'  : software,
                 'jobname'   : jobname,
                 'nodelist'  : '+'.join([node.split('/')[0] for node in hostlist.split('+')]),
                 'date'      : datetime.fromtimestamp(date_ts).strftime("%Y-%m-%d %H:%M:%S")}
      modA.append(entryT)
      ### datetime.utcfromtimestamp(date_ts)

  def report_by(self, args):
    resultA = []
    headerA, headerT, fmtT, orderT = SoftwareFormat(args)
    hline  = list(map(lambda x: "-"*len(x), headerT))
    resultA.append(headerT)
    resultA.append(hline)

    modA = self.__modA

    if args.sort[0] == '_':
      args.sort = args.sort[1:]
      sortA = sorted(modA, key=itemgetter(args.sort))
    else:
      sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    if args.log:
      resultA = []
      import numpy
      date_list = [ x['date'] for x in sortA ] 
      u_year = numpy.unique(map(lambda x: x.split('-')[0], date_list))
      u_month = numpy.unique(map(lambda x: x.split('-')[1], date_list))
      if len(u_year) == 1 and len(u_month) == 1: 
        for i in range(num):
          sortA[i]['year'] = u_year[0]
          sortA[i]['month'] = u_month[0]
          resultA.append(sortA[i])
      else: 
          print("Searching across multiple months is not available")
      return resultA

    for i in range(num):
      entryT = sortA[i]
      resultA.append(list(map(lambda x, y: x % entryT[y], fmtT, orderT)))

    statA = {'num': len(sortA),
             'cpuhours': sum([x['cpuhours'] for x in sortA])}
    if not args.jobs:
        statA['jobs'] = sum([x['jobs'] for x in sortA])
    return [headerA, resultA, statA]

class Job:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args):
    items = ['username','groupname','account','jobname','nproc','nodes','queue','submit_ts','start_ts','end_ts','cput_sec','walltime_sec','hostlist','exit_status','sw_app','script']
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

    ### grafana job view
    # https://grafana.osc.edu/d/qc1PWAUWz/cluster-metrics?orgId=1&from=1585143879000&to=1585230303000&var-cluster=owens&var-host=o0194&var-jobid=97739570
    isotimefmt = "%Y-%m-%dT%H:%M:%S"
#   grafana_url = 'https://grafana.osc.edu/d/qc1PWAUWz/cluster-metrics?orgId=3'
    grafana_url = 'https://grafana.osc.edu/d/qc1PWAUWz/cluster-metrics?'
    grafana_params = '&from=%s000&to=%s000&var-cluster=%s&var-jobid=%s' % \
                   (entryT['start_ts'], entryT['end_ts'], args.syshost, args.jobid)
    for k in ['submit_ts', 'start_ts', 'end_ts']:
      entryT[k] = datetime.fromtimestamp(entryT[k]).strftime("%Y-%m-%d %H:%M:%S")

    import urllib.parse
    hosts = []
    for h in entryT['hostlist'].split('+'):
      host = h.split('/')[0]
      hosts.append(host)
      print(host + ':')
      print(grafana_url + urllib.parse.quote(grafana_params + '&var-host=%s' % host, safe='=+&'))
      modA.append(entryT)

    #print('Compare: ')
    #print(ganglia_url + urllib.quote(compare_host + '|'.join(hosts), safe='=+&'))

  def report_by(self):
    modA = self.__modA
    #print(json.dumps(modA[0], indent=4, sort_keys=True))
    return modA
"""
    import requests
    import time
    params = (
      ('c', args.syshost.capitalize()),
      ('h', '%s.ten.osc.edu' % entryT['hostlist'].split('/')[0]),
      ('r', 'custom'),
      ('z', 'default'),
#     ('jr', ''),
#     ('js', ''),
#     ('st', '1552513647'),     # time at request
      ('cs', time.strftime('%m/%d/%Y %H:%M', time.localtime(int(entryT['start_ts'])))),
      ('ce', time.strftime('%m/%d/%Y %H:%M', time.localtime(int(entryT['end_ts'])))),
#     ('event', 'hide'),
#     ('ts', '0'),
#     ('v', '4722176000'),
#     ('m', 'mem_s_unreclaimable'),
      ('m', '%s' % args.gmetric),
#     ('vl', 'Bytes'),
#     ('ti', 'SUnreclaim'),
      ('json', '1'),
    )
    response = requests.get('https://ganglia.osc.edu/graph.php', params=params, auth=(args.webuser,args.webpass))
    #print(response.url)
    #print(response.content)
    json_data = json.loads(response.content)
    #print(json_data[0]['datapoints'])
    modA.append(json_data[0]['datapoints'])
"""

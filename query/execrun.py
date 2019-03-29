from operator import itemgetter
from .util import get_osc_group

def ExecRunFormat(args):
  headerA = "\nTop %s executables sorted by %s\n" % (str(args.num), args.sort)
  headerT = ["CoreHrs", "# Jobs", "# Users", "# GPUs", "# Cores", "# Threads", "ExecPath"]
  fmtT    = ["%.2f", "%d", "%d", "%d", "%d", "%d"]
  orderT  = ['corehours', 'jobs', 'users', 'n_gpus', 'n_cores', 'n_thds']
  if args.username:
    headerA = "\nTop %s executables used by users\n" % (str(args.num))
    headerT = ["CoreHrs", "# Jobs", "# GPUs", "# Cores", "# Threads", "Username", "ExecPath"]
    fmtT    = ["%.2f", "%d", "%d", "%d", "%d", "%s"]
    orderT  = ['corehours', 'jobs', 'n_gpus', 'n_cores', 'n_thds', 'users']
    if args.group:
       headerT.insert(-1, "Group")
  if args.user:
    headerA = "\nTop %s executables used by %s\n" % (str(args.num), args.user)
    headerT = ["CoreHrs", "# Jobs", "# GPUs", "# Cores", "# Threads", "ExecPath"]
    fmtT    = ["%.2f", "%d", "%d", "%d", "%d"]
    orderT  = ['corehours', 'jobs', 'n_gpus', 'n_cores', 'n_thds']
  if args.jobs:
    headerA = "\nFirst %s jobs sorted by %s\n" % (str(args.num), args.sort)
    if args.user:
      headerA = "\nFirst %s jobs used by %s\n" % (str(args.num), args.user)
    headerT = ["Date", "JobID", "CoreHrs", "# GPUs", "# Cores", "# Threads", "ExecPath"]
    fmtT    = ["%s", "%s", "%.2f", "%d", "%d", "%d"]
    orderT  = ['date', 'jobs', 'corehours', 'n_gpus', 'n_cores', 'n_thds']

  headerA += '\n'
  if args.sql != '%':
    headerA += '* Search pattern: %s\n' % args.sql
  headerA += '* WARNING: CoreHrs is executable walltime x # cores x # threads, not actual CPU utilization\n'

  return [headerA, headerT, fmtT, orderT]

class ExecRun:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    select_runtime = "ROUND(SUM(run_time*num_cores*num_threads)/3600,2) as corehours, "
    select_jobs  = "COUNT(DISTINCT(job_id)) as n_jobs, "
    select_user  = "COUNT(DISTINCT(user)) as n_users, "
    search_user  = ""
    search_gpu   = ""
    group_by     = "group by executables"
    if args.user or args.username:
      select_user = "user, "
      if args.user:
        search_user = "and user like '%s'" % args.user
        args.group = False
      if args.username:
        group_by = "group by user, executables"

    if args.jobs:
      select_runtime = "ROUND(run_time*num_cores*num_threads/3600,2) as corehours, "
      select_user = "user, "
      select_jobs = "job_id, "
      group_by = ""
      args.sort = 'date' if not args.sort else args.sort
    
    if args.gpu:
      search_gpu  = "and num_gpus > 0 "

    args.sort = 'corehours' if not args.sort else args.sort

    query = """ SELECT """ + \
    select_runtime + \
    select_jobs + \
    select_user + \
    """
    num_gpus                            as n_gpus,
    num_cores                           as n_cores,
    num_threads                         as n_thds,
    module_name                         as modules,
    exec_path                           as executables,
    date
    from xalt_run where syshost like %s
    """ + \
    search_user + \
    """
    and exec_path like %s
    and date >= %s and date <= %s
    """ + \
    search_gpu + \
    group_by

    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.sql, startdate, enddate))
    resultA = cursor.fetchall()
    modA = self.__modA
    for corehours, jobs, users, n_gpus, n_cores, n_thds, modules, executables, date in resultA:
      entryT = { 'corehours' : corehours,
                 'jobs'      : jobs,
                 'users'     : users,
                 'n_gpus'    : n_gpus,
                 'n_cores'   : n_cores,
                 'n_thds'    : n_thds,
                 'modules'   : modules,
                 'executables' : executables,
                 'date'        : date }
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    headerA, headerT, fmtT, orderT = ExecRunFormat(args)
    hline  = map(lambda x: "-"*len(x), headerT)
    resultA.append(headerT)
    resultA.append(hline)

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(map(lambda x, y: x % entryT[y], fmtT, orderT))
      resultA[-1].append(entryT['executables'] + " [%s]" % entryT['modules'])
      if args.group:
        group = get_osc_group(entryT['users'])
        resultA[-1].insert(-1, group)

    statA = {'num': len(sortA),
             'corehours': sum([x['corehours'] for x in sortA])}
    if not args.jobs:
        statA['jobs'] = sum([x['jobs'] for x in sortA])
    return [headerA, resultA, statA]

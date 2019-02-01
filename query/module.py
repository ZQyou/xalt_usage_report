from operator import itemgetter
from .util import get_osc_group

def ModuleFormat(args):
  headerA = "\nTop %s  modules sorted by %s\n" % (str(args.num), args.sort)
  headerT = ["CoreHrs", "# Jobs", "# Users", "# GPUs", "# Cores", "# Threads", "Modules"]
  fmtT    = ["%.2f", "%d", "%d", "%d", "%d", "%d", "%s"]
  orderT  = ['corehours', 'jobs', 'users', 'n_gpus', 'n_cores', 'n_thds', 'modules']
  if args.username:
    headerA = "\nTop %s modules used by users\n" % (str(args.num))
    headerT = ["CoreHrs", "# Jobs", "# GPUs", "# Cores", "# Threads", "Username", "Modules"]
    fmtT    = ["%.2f", "%d", "%d", "%d", "%d", "%s", "%s"]
    orderT  = ['corehours', 'jobs', 'n_gpus', 'n_cores', 'n_thds', 'users', 'modules']
    if args.group:
       headerT.insert(-1, "Group")
  if args.user:
    headerA = "\nTop %s modules used by %s\n" % (str(args.num), args.user)
    headerT = ["CoreHrs", "# Jobs", "# GPUs", "# CoreHrs", "# Threads", "Modules"]
    fmtT    = ["%.2f", "%d", "%d", "%d", "%d", "%s"]
    orderT  = ['corehours', 'jobs', 'n_gpus', 'n_cores', 'n_thds', 'modules']
  if args.jobs:
    headerA = "\nFirst %s jobs sorted by %s\n" % (str(args.num), args.sort)
    if args.user:
      headerA = "\nFirst %s jobs used by %s\n" % (str(args.num), args.user)
    headerT = ["Date", "JobID", "CoreHrs", "# GPUs", "# Cores", "# Threads", "Modules"]
    fmtT    = ["%s", "%s", "%.2f", "%d", "%d", "%d", "%s"]
    orderT  = ['date', 'jobs', 'corehours', 'n_gpus', 'n_cores', 'n_thds', 'modules']

  headerA += '\n'
  if args.sql != '%':
    headerA += '* Search pattern: %s\n' % args.sql
  headerA += '* WARNING: CoreHrs is executable walltime x # cores x # threads, not actual CPU utilization\n'

  return [headerA, headerT, fmtT, orderT]


class Module:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    select_runtime = "ROUND(SUM(run_time*num_cores*num_threads)/3600,2) as corehours, "
    select_jobs  = "COUNT(DISTINCT(job_id)) as n_jobs, "
    select_user  = "COUNT(DISTINCT(user)) as n_users, "
    search_user  = ""
    search_gpu   = ""
    group_by     = "group by modules"
    if args.user or args.username:
      select_user = "user, "
      if args.user:
        search_user = "and user like '%s'" % args.user
        args.group = False
      if args.username:
        group_by = "group by user, modules"

    if args.jobs:
      select_runtime = "ROUND(run_time*num_cores*num_threads/3600,2) as corehours, "
      select_user = "user, "
      select_jobs = "job_id, "
      group_by = ""
      args.sort = 'date' if not args.sort else args.sort
      args.group = False
    
    if args.gpu:
      search_gpu  = "and num_gpus > 0"

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
    date
    from xalt_run where syshost like %s
    """ + \
    search_user + \
    """
    and module_name like %s
    and date >= %s and date <= %s and module_name is not null
    """ + \
    search_gpu + \
    group_by

    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.sql, startdate, enddate))
    resultA = cursor.fetchall()
    modA = self.__modA
    for corehours, jobs, users, n_gpus, n_cores, n_thds, modules, date in resultA:
      entryT = { 'corehours' : corehours,
                 'jobs'      : jobs,
                 'users'     : users,
                 'n_gpus'    : n_gpus,
                 'n_cores'   : n_cores,
                 'n_thds'    : n_thds,
                 'modules'   : modules,
                 'date'      : date }
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    headerA, headerT, fmtT, orderT = ModuleFormat(args)
    hline  = map(lambda x: "-"*len(x), headerT)
    resultA.append(headerT)
    resultA.append(hline)

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(map(lambda x, y: x % entryT[y], fmtT, orderT))
      if args.group:
        group = get_osc_group(entryT['users'])
        resultA[-1].insert(-1, group)

    statA = {'num': len(sortA),
             'corehours': sum([x['corehours'] for x in sortA])}
    if not args.jobs:
        statA['jobs'] = sum([x['jobs'] for x in sortA])
    return [headerA, resultA, statA]

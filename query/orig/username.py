from operator import itemgetter
from .util import get_osc_group

class UserCountbyModule:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    has_gpu = "and num_gpus > 0" if args.gpu else ""
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores*num_threads/3600),2) as corehours,
    COUNT(DISTINCT(job_id))             as n_jobs,
    num_gpus                            as n_gpus,
    num_cores                           as n_cores,
    num_threads                         as n_thds,
    module_name                         as modules,
    user                                as usernames,
    date
    from xalt_run where syshost like %s
    and module_name like %s
    and date >= %s and date <= %s and  module_name is not null
    """ + \
    has_gpu + \
    """
    group by usernames, modules
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.sql, startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, n_gpus, n_cores, n_thds, modules, usernames, date in resultA:
      entryT = { 'corehours' : corehours,
                 'n_jobs'    : n_jobs,
                 'n_gpus'    : n_gpus,
                 'n_cores'   : n_cores,
                 'n_thds'    : n_thds,
                 'modules'   : modules,
                 'usernames' : usernames}
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    header = ["CoreHrs", "# Jobs", "# GPUs", "# Cores", "# Threads", "Username", "Modules"]
    if args.group:
      header.insert(-1, "Group")
    hline  = map(lambda x: "-"*len(x), header)
    resultA.append(header)
    resultA.append(hline)

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    fmtT = ["%.2f", "%d", "%d", "%d", "%d", "%s", "%s"]
    orderT = ['corehours', 'n_jobs', 'n_gpus', 'n_cores', 'n_thds', 'usernames', 'modules']
    for i in range(num):
      entryT = sortA[i]
      resultA.append(map(lambda x, y: x % entryT[y], fmtT, orderT))
      if args.group:
        group = get_osc_group(entryT['usernames'])
        resultA[-1].insert(-1, group)
    
    statA = {'num': len(sortA),
             'corehours': sum([x['corehours'] for x in sortA]),
             'jobs': sum([x['n_jobs'] for x in sortA])}
    return [resultA, statA]

class UserCountbyExecRun:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    has_gpu = "and num_gpus > 0" if args.gpu else ""
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores*num_threads/3600),2) as corehours,
    COUNT(DISTINCT(job_id))             as n_jobs,
    num_gpus                            as n_gpus,
    num_cores                           as n_cores,
    num_threads                         as n_thds,
    exec_path                           as executables,
    module_name                         as modules,
    user                                as usernames,
    date
    from xalt_run where syshost like %s
    and exec_path like %s
    and date >= %s and date <= %s
    """ + \
    has_gpu + \
    """
    group by usernames, executables
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, '%'+args.sql+'%', startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, n_gpus, n_cores, n_thds, executables, modules, usernames, date in resultA:
      entryT = { 'corehours'   : corehours,
                 'n_jobs'      : n_jobs,
                 'n_gpus'      : n_gpus,
                 'n_cores'     : n_cores,
                 'n_thds'      : n_thds,
                 'executables' : executables,
                 'modules'     : modules,
                 'usernames'   : usernames}
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    header =["CoreHrs", "# Jobs", "# GPUs", "# Cores", "# Threads", "Username", "ExecPath"]
    if args.group:
      header.insert(-1, "Group")
    hline  = map(lambda x: "-"*len(x), header)
    resultA.append(header)
    resultA.append(hline)

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    fmtT = ["%.2f", "%d", "%d", "%d", "%d", "%s"]
    orderT = ['corehours', 'n_jobs', 'n_gpus', 'n_cores', 'n_thds', 'usernames']
    for i in range(num):
      entryT = sortA[i]
      resultA.append(map(lambda x, y: x % entryT[y], fmtT, orderT))
      if args.group:
        group = get_osc_group(entryT['usernames'])
        resultA[-1].append(group)
      resultA[-1].append(entryT['executables'] + " [%s]" % entryT['modules'])
    
    statA = {'num': len(sortA),
             'corehours': sum([x['corehours'] for x in sortA]),
             'jobs': sum([x['n_jobs'] for x in sortA])}
    return [resultA, statA]

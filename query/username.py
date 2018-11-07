from operator import itemgetter

class UserCountbyModule:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    has_gpu = "and num_gpus > 0" if args.gpu else ""
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores/3600)) as corehours,
    count(date)                         as n_jobs,
    num_gpus                            as n_gpus,
    module_name                         as modules,
    user                                as usernames
    from xalt_run where syshost like %s
    and module_name like %s
    and date >= %s and date < %s and  module_name is not null
    """ + \
    has_gpu + \
    """
    group by usernames, modules
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.sql, startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, n_gpus, modules, usernames in resultA:
      entryT = { 'corehours' : corehours,
                 'n_jobs'    : n_jobs,
                 'n_gpus'    : n_gpus,
                 'modules'   : modules,
                 'usernames' : usernames}
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs", "# GPUs", "User Name", "Modules"])
    resultA.append(["-------", "------", "------", "---------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % entryT['corehours'], "%d" % entryT['n_jobs'], "%d" % entryT['n_gpus'], entryT['usernames'], entryT['modules']])
    
    return resultA

class UserCountbyExecRun:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    has_gpu = "and num_gpus > 0" if args.gpu else ""
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores/3600)) as corehours,
    count(date)                         as n_jobs,
    num_gpus                            as n_gpus,
    exec_path                           as executables,
    module_name                         as modules,
    user                                as usernames
    from xalt_run where syshost like %s
    and LOWER(SUBSTRING_INDEX(exec_path,'/',-1)) like %s
    and date >= %s and date < %s
    """ + \
    has_gpu + \
    """
    group by usernames, executables
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, '%'+args.sql+'%', startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, n_gpus, executables, modules, usernames in resultA:
      entryT = { 'corehours'   : corehours,
                 'n_jobs'      : n_jobs,
                 'n_gpus'      : n_gpus,
                 'executables' : executables,
                 'modules'     : modules,
                 'usernames'   : usernames}
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs", "# GPUs", "User Name", "ExecPath"])
    resultA.append(["-------", "------", "------", "---------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % entryT['corehours'],  "%d" % entryT['n_jobs'] , "%d" % entryT['n_gpus'], entryT['usernames'], entryT['executables'] + " (%s)" % entryT['modules']])
    
    return resultA


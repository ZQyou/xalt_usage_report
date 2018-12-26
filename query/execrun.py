from operator import itemgetter

class ExecRunCountbyName:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    has_gpu = "and num_gpus > 0" if args.gpu else ""
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores/3600),2) as corehours,
    count(date)                         as n_jobs,
    COUNT(DISTINCT(user))               as n_users,
    num_gpus                            as n_gpus,
    module_name                         as modules,
    exec_path                           as executables
    from xalt_run where syshost like %s
    and exec_path like %s
    and date >= %s and date <= %s
    """ + \
    has_gpu + \
    """
    group by executables
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.sql, startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, n_users, n_gpus, modules, executables in resultA:
      entryT = { 'corehours' : corehours,
                 'n_jobs'    : n_jobs,
                 'n_users'   : n_users,
                 'n_gpus'    : n_gpus,
                 'modules'   : modules,
                 'executables' : executables}
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs", "# Users", "# GPUs", "ExecPath"])
    resultA.append(["-------", "------", "-------", "------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.2f" % entryT['corehours'],  "%d" % entryT['n_jobs'] , "%d" % entryT['n_users'],  "%d" % entryT['n_gpus'], entryT['executables'] + " (%s)" % (entryT['modules'])])
    
    return resultA

class ExecRunCountbyUser:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    has_gpu = "and num_gpus > 0" if args.gpu else ""
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores/3600),2) as corehours,
    count(date)                         as n_jobs,
    num_gpus                            as n_gpus,
    module_name                         as modules,
    exec_path                           as executables,
    user
    from xalt_run where syshost like %s
    and user like %s
    and date >= %s and date <= %s
    """ + \
    has_gpu + \
    """
    group by executables
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.user, startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, n_gpus, modules, executables, user in resultA:
      entryT = { 'corehours' : corehours,
                 'n_jobs'    : n_jobs,
                 'n_gpus'    : n_gpus,
                 'modules'   : modules,
                 'executables' : executables }
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs", "# GPUs", "ExecPath"])
    resultA.append(["-------", "------", "------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.2f" % entryT['corehours'], "%d" % entryT['n_jobs'], "%d" % entryT['n_gpus'],  entryT['executables'] + " (%s)" % (entryT['modules'])])
    
    return resultA

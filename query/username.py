from operator import itemgetter

class UserCountbyModule:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores/3600)) as corehours,
    count(date)                         as n_jobs,
    module_name                         as modules,
    user                                as usernames
    from xalt_run where syshost like %s
    and module_name like %s
    and date >= %s and date < %s and  module_name is not null
    group by usernames, modules
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.sql, startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, modules, usernames in resultA:
      entryT = { 'corehours' : corehours,
                 'n_jobs'    : n_jobs,
                 'modules'   : modules,
                 'usernames' : usernames}
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs","Modules", "User Name"])
    resultA.append(["-------", "------","-------", "---------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % (entryT['corehours']),  "%d" % (entryT['n_jobs']) , entryT['modules'], entryT['usernames']])
    
    return resultA

class UserCountbyExecRun:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores/3600)) as corehours,
    count(date)                         as n_jobs,
    exec_path                           as executables,
    module_name                         as modules,
    user                                as usernames
    from xalt_run where syshost like %s
    and LOWER(SUBSTRING_INDEX(exec_path,'/',-1)) like %s
    and date >= %s and date < %s
    group by usernames, executables
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, '%'+args.sql+'%', startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, executables, modules, usernames in resultA:
      entryT = { 'corehours'   : corehours,
                 'n_jobs'      : n_jobs,
                 'executables' : executables,
                 'modules'     : modules,
                 'usernames'   : usernames}
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs","User Name", "ExecPath"])
    resultA.append(["-------", "------","---------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % (entryT['corehours']),  "%d" % (entryT['n_jobs']) , entryT['usernames'], entryT['executables'] + " (%s)" % (entryT['modules'])])
    
    return resultA


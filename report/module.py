from operator import itemgetter

class ModuleExec:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores/3600)) as corehours,
    count(date)                         as n_jobs,
    COUNT(DISTINCT(user))               as n_users,
    module_name                         as modules
    from xalt_run where syshost like %s
    and date >= %s and date <= %s and  module_name is not null
    group by modules
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, n_users, modules in resultA:
      entryT = { 'corehours' : corehours,
                 'n_jobs'    : n_jobs,
                 'n_users'   : n_users,
                 'modules'   : modules }
      modA.append(entryT)

  def report_by(self, args, sort_key):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs","# Users", "Modules"])
    resultA.append(["-------", "------","-------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(sort_key), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % (entryT['corehours']),  "%d" % (entryT['n_jobs']) , "%d" %(entryT['n_users']), entryT['modules']])
    
    return resultA

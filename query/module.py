from operator import itemgetter

class ModuleCountbyName:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    has_gpu = "and num_gpus > 0" if args.gpu else ""
    query = """
    SELECT 
    ROUND(SUM(run_time*num_cores/3600)) as corehours,
    count(date)                         as n_jobs,
    COUNT(DISTINCT(user))               as n_users,
    num_gpus                            as n_gpus,
    module_name                         as modules
    from xalt_run where syshost like %s
    and module_name like %s
    and date >= %s and date < %s and  module_name is not null
    """ + \
    has_gpu +\
    """
    group by modules
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.sql, startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, n_users, n_gpus, modules in resultA:
      entryT = { 'corehours' : corehours,
                 'n_jobs'    : n_jobs,
                 'n_users'   : n_users,
                 'n_gpus'    : n_gpus,
                 'modules'   : modules }
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs", "# Users", "# GPUs", "Modules"])
    resultA.append(["-------", "------", "-------", "------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % entryT['corehours'],  "%d" % entryT['n_jobs'] , "%d" % entryT['n_users'], "%d" % entryT['n_gpus'], entryT['modules']])
    
    return resultA

class ModuleCountbyUser:
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
    user    
    from xalt_run where syshost like %s
    and user like %s
    and date >= %s and date < %s and  module_name is not null
    """ + \
    has_gpu + \
    """
    group by modules
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.user, startdate, enddate))
    resultA = cursor.fetchall()
    modA   = self.__modA
    for corehours, n_jobs, n_gpus, modules, user in resultA:
      entryT = { 'corehours' : corehours,
                 'n_jobs'    : n_jobs,
                 'n_gpus'    : n_gpus,
                 'modules'   : modules }
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs", "# GPUs", "Modules"])
    resultA.append(["-------", "------", "------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % entryT['corehours'],  "%d" % entryT['n_jobs'], "%d" % entryT['n_gpus'], entryT['modules']])
    
    return resultA

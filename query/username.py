from operator import itemgetter
from subprocess import Popen, PIPE
from re import search, compile, IGNORECASE

group_re = compile('Primary Group:\s+(.*)\n', IGNORECASE)
def get_osc_group(username):
  process = Popen(['OSCfinger %s' % username], shell=True, stdout=PIPE, stderr=PIPE)
  stdout, stderr = process.communicate()
  return search(group_re, stdout).group(1)

class UserCountbyModule:
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
    user                                as usernames
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
    for corehours, n_jobs, n_gpus, modules, usernames in resultA:
      entryT = { 'corehours' : corehours,
                 'n_jobs'    : n_jobs,
                 'n_gpus'    : n_gpus,
                 'modules'   : modules,
                 'usernames' : usernames}
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    if args.group:
      resultA.append(["CoreHrs", "# Jobs", "# GPUs", "Username", "Group", "Modules"])
      resultA.append(["-------", "------", "------", "--------", "-----", "-------"])
    else:
      resultA.append(["CoreHrs", "# Jobs", "# GPUs", "Username", "Modules"])
      resultA.append(["-------", "------", "------", "--------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      if args.group:
        group = get_osc_group(entryT['usernames'])
        resultA.append(["%.2f" % entryT['corehours'], "%d" % entryT['n_jobs'], "%d" % entryT['n_gpus'], entryT['usernames'],  group, entryT['modules']])
      else:
        resultA.append(["%.2f" % entryT['corehours'], "%d" % entryT['n_jobs'], "%d" % entryT['n_gpus'], entryT['usernames'], entryT['modules']])
    
    return resultA

class UserCountbyExecRun:
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
    exec_path                           as executables,
    module_name                         as modules,
    user                                as usernames
    from xalt_run where syshost like %s
    and LOWER(SUBSTRING_INDEX(exec_path,'/',-1)) like %s
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
    if args.group:
      resultA.append(["CoreHrs", "# Jobs", "# GPUs", "Username", "Group", "ExecPath"])
      resultA.append(["-------", "------", "------", "--------", "-----", "-------"])
    else:
      resultA.append(["CoreHrs", "# Jobs", "# GPUs", "Username", "ExecPath"])
      resultA.append(["-------", "------", "------", "--------", "-------"])

    modA = self.__modA
    sortA = sorted(modA, key=itemgetter(args.sort), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      if args.group:
        group = get_osc_group(entryT['usernames'])
        resultA.append(["%.2f" % entryT['corehours'],  "%d" % entryT['n_jobs'] , "%d" % entryT['n_gpus'], entryT['usernames'], group, entryT['executables'] + " (%s)" % entryT['modules']])
      else:
        resultA.append(["%.2f" % entryT['corehours'],  "%d" % entryT['n_jobs'] , "%d" % entryT['n_gpus'], entryT['usernames'], entryT['executables'] + " (%s)" % entryT['modules']])
    
    return resultA


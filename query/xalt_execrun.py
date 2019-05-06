from operator import itemgetter
from .util import get_osc_group
from xalt_sw_mapping import sw_mapping

def ExecRunFormat(args):
  top_thing = "software/executables" if args.sw else "executable paths"
  headerA = "\nTop %s %s sorted by %s\n" % (str(args.num), top_thing, args.sort)
  headerT = ["CPUHrs", "NodeHrs", "# Jobs", "# Users"]
  fmtT    = ["%.2f", "%.2f", "%d", "%d" ]
  orderT  = ['cpuhours', 'nodehours', 'jobs', 'users']
  if args.username:
    headerA = "\nTop %s %s used by users\n" % (str(args.num), top_thing)
    headerT = ["CPUHrs", "NodeHrs", "# Jobs", "Username"]
    fmtT    = ["%.2f", "%.2f", "%d", "%s"]
    orderT  = ['cpuhours', 'nodehours', 'jobs', 'users']
    if args.group:
       headerT.insert(-1, "Group")
  if args.user:
    headerA = "\nTop %s %s used by %s\n" % (str(args.num), top_thing, args.user)
    headerT = ["CPUHrs", "NodeHrs", "# Jobs"]
    fmtT    = ["%.2f", "%.2f", "%d"]
    orderT  = ['cpuhours', 'nodehours', 'jobs']
  if args.jobs:
    headerA = "\nFirst %s jobs sorted by %s\n" % (str(args.num), args.sort)
    if args.user:
      headerA = "\nFirst %s jobs used by %s\n" % (str(args.num), args.user)
    headerT = ["Date", "JobID", "CPUHrs", "NodeHrs", "# GPUs", "# Cores", "# Threads"]
    fmtT    = ["%s", "%s", "%.2f", "%.2f", "%d", "%d", "%d"]
    orderT  = ['date', 'jobs', 'cpuhours', 'nodehours', 'n_gpus', 'n_cores', 'n_thds']

  headerT += ["Software/Executable"] if args.sw else ["ExecPath"]

  headerA += '\n'
  if args.sql != '%':
    headerA += '* Search pattern: %s\n' % args.sql
  if args.gpu:
    headerA += '* GPU jobs only\n'
  headerA += '* WARNING: CPUHrs is executable walltime x # cores x # threads, not actual CPU utilization\n'

  return [headerA, headerT, fmtT, orderT]

class ExecRun:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    select_runtime = """
    ROUND(SUM(run_time*num_cores*num_threads)/3600,2) AS cpuhours,
    ROUND(SUM(run_time*num_nodes)/3600,2) AS nodehours,
    """
    select_jobs  = "COUNT(DISTINCT(job_id)) AS n_jobs, "
    select_user  = "COUNT(DISTINCT(user)) AS n_users, "
    search_user  = ""
    search_gpu   = ""
    group_by     = "GROUP BY executables"
    if args.user or args.username:
      select_user = "user, "
      if args.user:
        search_user = "and user LIKE '%s'" % args.user
        args.group = False
      if args.username:
        group_by = "GROUP BY user, executables"

    if args.jobs:
      select_runtime = """
      ROUND(run_time*num_cores*num_threads/3600,2) AS cpuhours,
      ROUND(run_time*num_nodes/3600,2) AS nodehours,
      """
      select_user = "user, "
      select_jobs = "job_id, "
      group_by = ""
      args.sort = 'date' if not args.sort else args.sort
    
    if args.gpu:
      search_gpu  = "and num_gpus > 0 "

    args.sort = 'cpuhours' if not args.sort else args.sort


    select_sw =  "exec_path AS executables, "
    search_sw = "and LOWER(exec_path) LIKE %s "
    sA = []
    if args.sw:
      equiv_patternA = sw_mapping()
      sA.append("CASE ")
      for entry in equiv_patternA:
        regexp = entry[0].lower()
        sw     = entry[1]
        s      = "WHEN LOWER(SUBSTRING_INDEX(exec_path,'/',-1)) REGEXP '%s' THEN '%s' " % (regexp, sw)
        sA.append(s)
      
      sA.append(" ELSE SUBSTRING_INDEX(exec_path,'/',-1) END ")
      select_sw = "".join(sA) + " AS executables, "
      search_sw = "and LOWER(" + "".join(sA) + ") LIKE %s "

    query = """SELECT """ + \
    select_runtime + \
    select_jobs + \
    select_user + \
    """
    num_gpus    AS n_gpus,
    num_cores   AS n_cores,
    num_threads AS n_thds,
    module_name AS modules,
    """ + \
    select_sw + \
    """
    date
    FROM xalt_run WHERE syshost LIKE %s
    """ + \
    search_user + \
    search_sw + \
    """
    and date >= %s and date <= %s
    """ + \
    search_gpu + \
    group_by

    #print(query)
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, args.sql.lower(), startdate, enddate))
    resultA = cursor.fetchall()
    modA = self.__modA
    for cpuhours, nodehours, jobs, users, n_gpus, n_cores, n_thds, modules, executables, date in resultA:
      entryT = { 'cpuhours' : cpuhours,
                 'nodehours' : nodehours,
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
      u_year = numpy.unique(map(lambda x: x.year, date_list))
      u_month = numpy.unique(map(lambda x: '%02d' % x.month, date_list))
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
      resultA.append(map(lambda x, y: x % entryT[y], fmtT, orderT))
      if args.sw:
        resultA[-1].append(entryT['executables'])
      else:
        resultA[-1].append(entryT['executables'] + " [%s]" % entryT['modules'])
      if args.group:
        group = get_osc_group(entryT['users'])
        resultA[-1].insert(-1, group)

    statA = {'num': len(sortA),
             'cpuhours': sum([x['cpuhours'] for x in sortA])}
    if not args.jobs:
        statA['jobs'] = sum([x['jobs'] for x in sortA])
    return [headerA, resultA, statA]

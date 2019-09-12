from operator import itemgetter
from .util import get_osc_group

def LibraryFormat(args):
  top_thing = "libraries"
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
    headerT = ["Date", "JobID", "CPUHrs", "NodeHrs"]
    fmtT    = ["%s", "%s", "%.2f", "%.2f"]
    orderT  = ['date', 'jobs', 'cpuhours', 'nodehours']

  headerT += ["Libraies"]

  headerA += '\n'
  if args.sql != '%':
    headerA += '* Search pattern: %s\n' % args.sql
  if args.gpu:
    headerA += '* GPU jobs only\n'
  headerA += '* WARNING: CPUHrs is executable walltime x # cores x # threads, not actual CPU utilization\n'

  return [headerA, headerT, fmtT, orderT]

class Library:
  def __init__(self, cursor):
    self.__modA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    select_runtime = """
    ROUND(SUM(t3.run_time*t3.num_cores*num_threads)/3600,2) AS cpuhours,
    ROUND(SUM(t3.run_time*t3.num_nodes)/3600,2) AS nodehours,
    """
    select_jobs  = "COUNT(DISTINCT(t3.job_id)) AS n_jobs, "
    select_user  = "COUNT(DISTINCT(t3.user)) AS n_users, "
    search_user  = ""
    search_gpu   = ""
#   group_by     = "GROUP BY t1.object_path"
    group_by     = "GROUP BY t1.module_name"
    if args.user or args.username:
      select_user = "t3.user, "
      if args.user:
        search_user = "and t3.user LIKE '%s'" % args.user
        args.group = False
      if args.username:
#       group_by = "GROUP BY t3.user, t1.object_path"
        group_by = "GROUP BY t3.user, t1.module_name"

    if args.jobs:
      select_runtime = """
      ROUND(t3.run_time*t3.num_cores*num_threads/3600,2) AS cpuhours,
      ROUND(t3.run_time*t3.num_nodes/3600,2) AS nodehours,
      """
      select_user = "t3.user, "
      select_jobs = "t3.job_id, "
      group_by = ""
      args.sort = 'date' if not args.sort else args.sort
    
    if args.gpu:
      search_gpu  = "and t3.num_gpus > 0 "

    args.sort = 'cpuhours' if not args.sort else args.sort

    query = """SELECT """ + \
    select_runtime + \
    select_jobs + \
    select_user + \
    """
    t1.object_path, 
    t1.module_name AS modules,
    """ + \
    """
    t3.date
    FROM xalt_object as t1, join_run_object as t2, xalt_run as t3 WHERE t3.syshost LIKE %s
    and (t1.module_name is not NULL and t1.module_name != 'NULL')
    and t1.obj_id = t2.obj_id and t2.run_id = t3.run_id
    """ + \
    search_user + \
    """
    and t3.date >= %s and t3.date <= %s
    """ + \
    search_gpu + \
    group_by

    #print(query)
    cursor  = self.__cursor
    #cursor.execute(query, (args.syshost, args.sql.lower(), startdate, enddate))
    cursor.execute(query, (args.syshost, startdate, enddate))
    resultA = cursor.fetchall()
    modA = self.__modA
    for cpuhours, nodehours, jobs, users, object_path, modules, date in resultA:
      entryT = { 'cpuhours' : cpuhours,
                 'nodehours' : nodehours,
                 'jobs'      : jobs,
                 'users'     : users,
                 'object_path' : object_path,
                 'modules'   : modules,
                 'date'        : date }
      modA.append(entryT)

  def report_by(self, args):
    resultA = []
    headerA, headerT, fmtT, orderT = LibraryFormat(args)
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
      resultA[-1].append(entryT['modules'])
      if args.group:
        group = get_osc_group(entryT['users'])
        resultA[-1].insert(-1, group)

    statA = {'num': len(sortA),
             'cpuhours': sum([x['cpuhours'] for x in sortA])}
    if not args.jobs:
        statA['jobs'] = sum([x['jobs'] for x in sortA])
    return [headerA, resultA, statA]

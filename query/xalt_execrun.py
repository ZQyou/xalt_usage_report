from operator import itemgetter
from .util import get_osc_group
from xalt_sw_mapping import sw_mapping
import pandas as pd
import time

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
  def __init__(self, connect):
    self.__modA  = []
    self.__conn = connect

  def build(self, args, startdate, enddate):
    search_user = ""
    if args.user:
      args.group = False
      search_user = "AND user LIKE '%s' " % args.user
    args.sort = 'cpuhours' if not args.sort else args.sort

    query = """SELECT
    date, run_time, job_id,
    user        AS users, 
    num_gpus    AS n_gpus,
    num_cores   AS n_cores,
    num_threads AS n_thds,
    num_nodes   AS n_nodes,
    module_name AS modules,
    exec_path   AS executables
    FROM xalt_run WHERE syshost LIKE %s
    AND date >= %s and date <= %s
    AND LOWER(exec_path) LIKE %s 
    """ + \
    search_user 
    #print(query)
    
    connect = self.__conn
    s_time = time.time()
    queryA = pd.read_sql(query, connect,
            params=(args.syshost, startdate, enddate, args.sql.lower()))
    print("Query time: %.2f" % float((time.time() - s_time)))

    queryA['cpuhours'] = queryA['run_time'] * queryA['n_thds'] * queryA['n_cores']
    queryA['nodehours'] = queryA['run_time'] * queryA['n_nodes']

    if args.sw:
      queryA['executables'] = queryA['executables'].replace(to_replace='/.*/', value='', regex=True)
      equiv_patternA = sw_mapping()
      for entry in equiv_patternA:
        queryA.loc[queryA['executables'].str.contains(entry[0]), 'executables'] = entry[1]

    dg = queryA.groupby(['users', 'executables']) if args.username else queryA.groupby('executables')
    df = dg['cpuhours'].sum().divide(3600).round(2).to_frame()
    df['nodehours'] = dg['nodehours'].sum().divide(3600).round(2)
    df['jobs'] = dg['job_id'].nunique()
    if not args.username:
      df['users'] = dg['users'].nunique()

    self.__modA = df.sort_values(by=args.sort, ascending=args.asc).reset_index().T.to_dict().values()

  def report_by(self, args):
    resultA = []
    headerA, headerT, fmtT, orderT = ExecRunFormat(args)
    hline  = map(lambda x: "-"*len(x), headerT)
    resultA.append(headerT)
    resultA.append(hline)

    modA = self.__modA
    num = min(int(args.num), len(modA))
    if args.log:
      resultA = []
      import numpy
      date_list = [ x['date'] for x in modA ] 
      u_year = numpy.unique(map(lambda x: x.year, date_list))
      u_month = numpy.unique(map(lambda x: '%02d' % x.month, date_list))
      if len(u_year) == 0 or len(u_month) == 0: 
        print("No data available in selected time range")
        print("or you are not on the right system")
      elif len(u_year) == 1 and len(u_month) == 1: 
        for i in range(num):
          modA[i]['year'] = u_year[0]
          modA[i]['month'] = u_month[0]
          resultA.append(modA[i])
      else: 
          print("Searching across multiple months is not available")
      return resultA

    for i in range(num):
      entryT = modA[i]
      resultA.append(map(lambda x, y: x % entryT[y], fmtT, orderT))
      if args.sw:
        resultA[-1].append(entryT['executables'])
      else:
        resultA[-1].append(entryT['executables'] + " [%s]" % entryT['modules'])
      if args.group:
        group = get_osc_group(entryT['users'])
        resultA[-1].insert(-1, group)

    statA = {'num': len(modA),
             'cpuhours': sum([x['cpuhours'] for x in modA])}
    if not args.jobs:
        statA['jobs'] = sum([x['jobs'] for x in modA])
    return [headerA, resultA, statA]

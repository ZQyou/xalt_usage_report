from xalt_name_mapping import name_mapping
from operator import itemgetter

class ExecRun:
  """ Holds the executables for a given date range """
  def __init__(self, cursor):
    self.__execA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    equiv_patternA = name_mapping()
    sA = []
    sA.append("SELECT CASE ")
    for entry in equiv_patternA:
      left  = entry[0].lower()
      right = entry[1]
      s     = "WHEN LOWER(SUBSTRING_INDEX(xalt_run.exec_path,'/',-1)) REGEXP '%s' then '%s' " % (left, right)
      sA.append(s)

    sA.append(" ELSE SUBSTRING_INDEX(xalt_run.exec_path,'/',-1) END ")
    sA.append(" AS execname, ROUND(SUM(run_time*num_cores/3600)) as totalcput, ")
    sA.append(" COUNT(date) as n_jobs, COUNT(DISTINCT(user)) as n_users ")
    sA.append("   FROM xalt_run ")
    sA.append("  WHERE syshost like '%s' ")
    sA.append("    AND date >= '%s' AND date < '%s' ")
    sA.append("  GROUP BY execname ORDER BY totalcput DESC")

    query  = "".join(sA) % (args.syshost, startdate, enddate)
    cursor = self.__cursor

    cursor.execute(query)
    resultA = cursor.fetchall()

    execA = self.__execA
    for execname, corehours, n_jobs, n_users in resultA:
      entryT = {'execname'  : execname,
                'corehours' : corehours,
                'n_jobs'    : n_jobs,
                'n_users'   : n_users}
      execA.append(entryT)

  def report_by(self, args, sort_key):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs","# Users", "Exec"])
    resultA.append(["-------", "------","-------", "----"])

    execA = self.__execA
    sortA = sorted(execA, key=itemgetter(sort_key), reverse=True)
    num = min(int(args.num), len(sortA))
    sumCH = 0.0
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % (entryT['corehours']),  "%d" % (entryT['n_jobs']) , "%d" %(entryT['n_users']), entryT['execname']])
      sumCH += entryT['corehours']
    
    return resultA, sumCH


class ExecRunLink:
  """ Holds the executables for a given date range """
  def __init__(self, cursor):
    self.__execA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate, compiler):
    equiv_patternA = name_mapping()
    sA = []
    sA.append("SELECT CASE ")
    for entry in equiv_patternA:
      left  = entry[0].lower()
      right = entry[1]
      s     = "WHEN LOWER(SUBSTRING_INDEX(t1.exec_path,'/',-1)) REGEXP '%s' then '%s' " % (left, right)
      sA.append(s)

    sA.append(" ELSE SUBSTRING_INDEX(t1.exec_path,'/',-1) END ")
    sA.append(" AS execname, ROUND(SUM(t1.run_time*t1.num_cores/3600)) as totalcput, ")
    sA.append(" COUNT(t1.date) as n_jobs, COUNT(DISTINCT(t1.user)) as n_users")
    sA.append("   FROM xalt_run as t1, xalt_link as t2 ")
    sA.append("  WHERE t1.syshost like '%s' ")
    sA.append("    AND t1.date >= '%s' AND t1.date <= '%s' ")
    sA.append("    AND t1.uuid = t2.uuid")
    sA.append("    AND t2.link_program = '%s' ")
    sA.append("  GROUP BY execname ORDER BY totalcput DESC")

    query  = "".join(sA) % (args.syshost, startdate, enddate, compiler)
    cursor = self.__cursor

    cursor.execute(query)
    resultA = cursor.fetchall()

    execA = self.__execA
    for execname, corehours, n_jobs, n_users in resultA:
      entryT = {'execname'  : execname,
                'corehours' : corehours,
                'n_jobs'    : n_jobs,
                'n_users'   : n_users}
      execA.append(entryT)

  def report_by(self, args, sort_key):
    resultA = []
    resultA.append(["CoreHrs", "# Jobs","# Users", "Exec"])
    resultA.append(["-------", "------","-------", "----"])

    execA = self.__execA
    sortA = sorted(execA, key=itemgetter(sort_key), reverse=True)
    num = min(int(args.num), len(sortA))
    sumCH = 0.0
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % (entryT['corehours']),  "%d" % (entryT['n_jobs']) , "%d" %(entryT['n_users']), entryT['execname']])
      sumCH += entryT['corehours']
    
    return resultA, sumCH

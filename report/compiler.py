from operator import itemgetter

class CompilerUsageByCount:
  def __init__(self, cursor):
    self.__linkA  = []
    self.__cursor = cursor
  def build(self, args, startdate, enddate):
    query = """
    SELECT link_program, count(date) as count FROM xalt_link
    WHERE build_syshost like %s
    AND   date >= %s AND date <= %s
    GROUP by link_program
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, startdate, enddate))
    resultA = cursor.fetchall()
    linkA   = self.__linkA
    for link_program, count in resultA:
      entryT = { 'count'        : count,
                 'link_program' : link_program }
      linkA.append(entryT)
    
  def report_by(self, args, sort_key):
    resultA = []
    resultA.append(["Count", "Link Program"])
    resultA.append(["-----", "------------"])

    linkA = self.__linkA
    sortA = sorted(linkA, key=itemgetter(sort_key), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%d" % (entryT['count']), entryT['link_program']])
    
    return resultA

class CompilerUsageByCoreHours:
  def __init__(self, cursor):
    self.__linkA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate):
    query = """
    SELECT
    ROUND(SUM(t1.run_time*t1.num_cores/3600.0)) as corehours,
    COUNT(t1.date)                              as n_runs,
    COUNT(DISTINCT(t1.user))                    as n_users,
    t2.link_program                             as link_program
    FROM xalt_run as t1, xalt_link as t2
    WHERE t1.uuid is not NULL
    AND   t1.uuid = t2.uuid
    and   t1.syshost like %s
    AND   t1.date >= %s and t1.date <= %s
    GROUP by link_program
    """
    cursor  = self.__cursor
    cursor.execute(query, (args.syshost, startdate, enddate))
    resultA = cursor.fetchall()
    linkA   = self.__linkA
    for corehours, n_runs, n_users, link_program in resultA:
      entryT = { 'corehours'   : corehours,
                 'n_users'     : n_users,
                 'n_runs'      : n_runs,
                 'link_program': link_program
               }
      linkA.append(entryT)
    

  def report_by(self, args, sort_key):
    resultA = []
    resultA.append(['CoreHrs', '# Users','# Runs','Link Program'])
    resultA.append(['-------', '-------','------','------------'])

    linkA = self.__linkA
    sortA = sorted(linkA, key=itemgetter(sort_key), reverse=True)
    num = min(int(args.num), len(sortA))
    for i in range(num):
      entryT = sortA[i]
      resultA.append(["%.0f" % (entryT['corehours']), "%d" % (entryT['n_users']), "%d" % (entryT['n_runs']), \
                      entryT['link_program']])
    return resultA


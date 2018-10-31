def shortName(full):
  idx = full.find('(')
  if (idx != -1):
    full = full[:idx]

  idx = full.rfind('/')
  if (idx == -1):
    return full
  return full[:idx]

class Libraries:

  def __init__(self, cursor):
    self.__libA  = []
    self.__cursor = cursor

  def build(self, args, startdate, enddate, queue="%"):
    
    query = "select ROUND(SUM(t3.num_cores*t3.run_time/3600.0)) as corehours,        \
                    COUNT(DISTINCT(t3.user)) as n_users,                             \
                    COUNT(t3.date) as n_runs, COUNT(DISTINCT(t3.job_id)) as n_jobs,  \
                    t1.object_path, t1.module_name as module                         \
                    from xalt_object as t1, join_run_object as t2, xalt_run as t3    \
                    where ( t1.module_name is not NULL and t1.module_name != 'NULL') \
                    and t1.obj_id = t2.obj_id and t2.run_id = t3.run_id              \
                    and t3.syshost like %s and t3.queue like %s                      \
                    and t3.date >= %s and t3.date < %s                               \
                    group by t1.object_path order by corehours desc"

    cursor  = self.__cursor
    cursor.execute(query,(args.syshost, queue, startdate, enddate))
    resultA = cursor.fetchall()

    libA = self.__libA
    for corehours, n_users, n_runs, n_jobs, object_path, module in resultA:
      entryT = { 'corehours'   : corehours,
                 'n_users'     : n_users,
                 'n_runs'      : n_runs,
                 'n_jobs'      : n_jobs,
                 'object_path' : object_path,
                 'module'      : module
               }
      libA.append(entryT)
      

    #q2 = "select ROUND(SUM(t3.num_cores*t3.run_time/3600.0)) as corehours, COUNT(DISTINCT(t3.user)) as n_users, COUNT(t3.date) as n_runs, COUNT(DISTINCT(t3.job_id)) as n_jobs, t1.object_path from xalt_object as t1, join_run_object as t2, xalt_run as t3  where t1.module_name is NULL and t1.obj_id = t2.obj_id and t2.run_id = t3.run_id and t3.date >= '2016-04-04' and t3.date < '2016-04-05' group by t1.object_path order by corehours desc"

  def report_by(self, args, sort_key):
    resultA = []
    resultA.append(['CoreHrs', '# Users','# Runs','# Jobs','Library Module'])
    resultA.append(['-------', '-------','------','------','--------------'])

    libA = self.__libA
    libT = {}

    for entryT in libA:
      module = entryT['module']
      if (module in libT):
        if (entryT['corehours'] > libT[module]['corehours']):
          libT[module] = entryT
      else:
        libT[module] = entryT
    
        
    for k, entryT in sorted(libT.iteritems(), key=lambda(k,v): v[sort_key], reverse=True):
      resultA.append(["%.0f" % (entryT['corehours']), "%d" % (entryT['n_users']), "%d" % (entryT['n_runs']), \
                      "%d" % (entryT['n_jobs']), entryT['module']])

    return resultA
  def group_report_by(self, args, sort_key):
    resultA = []
    resultA.append(['CoreHrs', '# Users','# Runs','# Jobs','Library Module'])
    resultA.append(['-------', '-------','------','------','--------------'])

    libA = self.__libA
    libT = {}

    for entryT in libA:
      module = entryT['module']
      if (module in libT):
        if (entryT['corehours'] > libT[module]['corehours']):
          libT[module] = entryT
      else:
        libT[module] = entryT
    
    groupT = {}
    for module in libT:
      sn = shortName(module)
      if (not sn in groupT):
        groupT[sn]           = libT[module]
        groupT[sn]['module'] = sn
      else:
        g_entry = groupT[sn]
        entry   = libT[module]
        for key in g_entry:
          if (key != "module"):
            g_entry[key] += entry[key]
        
    for k, entryT in sorted(groupT.iteritems(), key=lambda(k,v): v[sort_key], reverse=True):
      resultA.append(["%.0f" % (entryT['corehours']), "%d" % (entryT['n_users']), "%d" % (entryT['n_runs']), \
                      "%d" % (entryT['n_jobs']), entryT['module']])

    return resultA

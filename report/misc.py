from __future__ import print_function

def kinds_of_jobs(cursor, args, startdate, enddate):

  query = "SELECT ROUND(SUM(run_time*num_cores/3600)) as corehours,                \
                  COUNT(*) as n_runs, COUNT(DISTINCT(job_id)) as n_jobs,           \
                  COUNT(DISTINCT(user)) as n_users                                 \
                  from xalt_run where syshost like %s                              \
                  and date >= %s and date < %s and exec_type = %s "



  execKindA = [ ["sys",        "binary", "and module_name is not NULL"],
                ["usr"  ,      "binary", "and module_name is NULL"    ],
                ["sys-script", "script", "and module_name is not NULL"],
                ["usr-script", "script", "and module_name is NULL"    ]
              ]

  resultT = {}

  
  totalT  = {'corehours' : 0.0,
             'n_jobs'    : 0.0,
             'n_runs'    : 0.0}

  for entryA in execKindA:
    name   = entryA[0]
    kind   = entryA[1]
    q2     = query + entryA[2]
    cursor.execute(q2,(args.syshost, startdate, enddate, kind))

    if (cursor.rowcount == 0):
      print("Unable to get the number of", kind," jobs: Quitting!")
      sys.exit(1)
    
    row = cursor.fetchall()[0]
    core_hours = float(row[0] or 0.0)
    resultT[name] = {'corehours' : core_hours,
                     'n_runs'    : int(row[1]),
                     'n_jobs'    : int(row[2]),
                     'n_users'   : int(row[3]),
                     'name'      : name
                     }

    totalT['corehours'] += core_hours
    totalT['n_runs'   ] += int(row[1])
    totalT['n_jobs'   ] += int(row[2])


  resultA = []
  resultA.append(["Kind", "CoreHrs", "   ", "Runs", "   ", "Jobs", "   ", "Users"])
  resultA.append(["    ", "  N    ", " % ", " N  ", " % ", " N  ", " % ", "  N  "])
  resultA.append(["----", "-------", "---", "----", "---", "----", "---", "-----"])
  

     
  for k, entryT in sorted(resultT.iteritems(), key=lambda(k,v): v['corehours'], reverse=True):
    pSU = percent_str(entryT['corehours'], totalT['corehours'])
    pR  = percent_str(entryT['n_runs'],    float(totalT['n_runs']))
    pJ  = percent_str(entryT['n_jobs'],    float(totalT['n_jobs']))

    resultA.append([k,
      "%.0f" % (entryT['corehours']), pSU,
      entryT['n_runs'],               pR,
      entryT['n_jobs'],               pJ,
      entryT['n_users']])
                 

  resultA.append(["----", "-------", "---", "----", "---", "----", "---", " "])
  resultA.append(["Total", "%.0f" % (totalT['corehours']), "100", "%.0f" % (totalT['n_runs']), "100", "%.0f" % (totalT['n_jobs']), "100", " "])
  return resultA

def percent_str(entry, total):
  result = 0.0
  if (total != 0.0):
    result = "%.0f" % (100.0 * entry/total)
  return result


def running_other_exec(cursor, args, startdate, enddate):

  query = "SELECT ROUND(SUM(t1.num_cores*t1.run_time/3600.0)) as corehours, \
           COUNT(t1.date)                                     as n_runs,    \
           COUNT(DISTINCT(t1.user))                           as n_users    \
           FROM xalt_run AS t1, xalt_link AS t2                             \
           WHERE t1.uuid is not NULL and t1.uuid = t2.uuid                  \
           and t1.syshost like %s                                           \
           and t1.user != t2.build_user and t1.module_name is NULL          \
           and t1.date >= %s and t1.date <= %s"

  resultT = {}
  cursor.execute(query, (args.syshost, startdate, enddate));
  if (cursor.rowcount == 0):
    print("Unable to get the number of user != build_user: Quitting!")
    sys.exit(1)
    
  row = cursor.fetchall()[0]
  
  core_hours = float(row[0] or 0.0)

  resultT['diff'] = {'corehours' : core_hours,
                     'n_runs'    : int(row[1]),
                     'n_users'   : int(row[2])
                    }

  query = "SELECT ROUND(SUM(t1.num_cores*t1.run_time/3600.0)) as corehours, \
           COUNT(t1.date)                                     as n_runs,    \
           COUNT(DISTINCT(t1.user))                           as n_users    \
           FROM xalt_run AS t1, xalt_link AS t2                             \
           WHERE t1.uuid is not NULL and t1.uuid = t2.uuid                  \
           and t1.syshost like %s                                           \
           and t1.user = t2.build_user                                      \
           and t1.date >= %s and t1.date <= %s"
  
  cursor.execute(query, (args.syshost, startdate, enddate));
  if (cursor.rowcount == 0):
    print("Unable to get the number of user != build_user: Quitting!")
    sys.exit(1)
    
  row = cursor.fetchall()[0]
  resultT['same'] = {'corehours' : float(row[0] or 0.0),
                     'n_runs'    : int(row[1]),
                     'n_users'   : int(row[2])
                    }

  resultA = []
  resultA.append(["Kind", "CoreHrs", "# Runs", "# Users"])
  resultA.append(["----", "-------", "------", "-------"])

  resultA.append(["diff user","%.0f" % (resultT['diff']['corehours']), resultT['diff']['n_runs'], resultT['diff']['n_users']])
  resultA.append(["same user","%.0f" % (resultT['same']['corehours']), resultT['same']['n_runs'], resultT['same']['n_users']])

  return resultA

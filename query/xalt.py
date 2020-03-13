from .xalt_format import ExecRunFormat, ModuleFormat
from .xalt_sw_mapping import sw_mapping
from .util import get_osc_group
import pandas as pd
from time import time 
from glob import glob
import sys

database_path = '/fs/project/PZS0710/database'

class Xalt:
  def __init__(self, connect):
    self.__modA  = []
    self.__conn = connect
    self.__query = """SELECT
    date, run_time,
    job_id      AS jobs,
    user        AS users, 
    num_gpus    AS n_gpus,
    num_cores   AS n_cores,
    num_threads AS n_thds,
    num_nodes   AS n_nodes,
    module_name AS modules,
    exec_path   AS executables
    FROM xalt_run WHERE syshost LIKE %s
    AND date >= %s and date <= %s
    """
    self.__path = database_path
  
  def build(self, args, startdate, enddate):
    # Object and user searching
    sw_key = 'executables'
    sql_re = args.sql.lower()
    sw_re = None 
    if sql_re != '%':
       sw_re = sql_re.replace('%','')
       sw_re += '$' if sql_re[-1] != '%' else sw_re
    search_user = ""
    if args.user:
      args.group = False
      search_user = "AND user LIKE '%s' " % args.user

    query = self.__query + search_user 
    if args.module:
      sw_key = 'modules'
      query += ' AND LOWER(module_name) LIKE %s ' 
    else:
      query += ' AND LOWER(exec_path) LIKE %s ' 
    #print(query)     
 
    db_path = self.__path + '/xalt/%s' % args.syshost
    db_list = [ f.split('/')[-1] for f in glob(db_path + '/*.pq', recursive=False) ]

    connect = self.__conn
    queryA = df = q = None
    year0 = month0 = '0'

    print("\nData processing ....")
    print("=============")
    for i in range(len(startdate)):
      year, month = startdate[i].split('-')[0:2]
      if year0 != year or month0 != month:
        df = None
        year0 = year 
        month0 = month
      db_name = '%04d%02d.pq' % (int(year), int(month))
      t0 = time()
      with_db = False if args.nopq else (db_name in db_list)
      if with_db:
        if not isinstance(df, pd.DataFrame): 
          print("Loading %s" % db_name)
          df = pd.read_parquet(db_path + '/' + db_name, engine='pyarrow')
        # Filtering
        if sw_re and args.user:
          criteria = (df['date'] >= startdate[i]) & (df['date'] <= enddate[i]) & \
                     (df[sw_key].str.contains("(?i)%s" % sw_re)) & \
                     (df['users'] == args.user)
        elif sw_re:
          criteria = (df['date'] >= startdate[i]) & (df['date'] <= enddate[i]) & \
                     (df[sw_key].str.contains("(?i)%s" % sw_re)) 
        elif args.user:
          criteria = (df['date'] >= startdate[i]) & (df['date'] <= enddate[i]) & \
                     (df['users'] == args.user)
        else:
          criteria = (df['date'] >= startdate[i]) & (df['date'] <= enddate[i])
        # Avoid 'SettingWithCopyWarning'
        # https://maxpowerwastaken.github.io/blog/pandas_view_vs_copy/
        q = df.loc[df[criteria].index, :]
      else:
        print("Importing from xalt_run")
        t0 = time()
        q = pd.read_sql(query, connect,
              params=(args.syshost, startdate[i], enddate[i], sql_re))

      print("Query time (%s - %s): %.2fs" % (startdate[i], enddate[i], float(time() - t0)))

      queryA = queryA.append(q, ignore_index=True) if isinstance(queryA, pd.DataFrame) else q

    queryA['cpuhours'] = queryA['run_time'] * queryA['n_thds'] * queryA['n_cores']
    queryA['nodehours'] = queryA['run_time'] * queryA['n_nodes']

    print(queryA.info(verbose=False))

    t0 = time()
    if args.sw:
      queryA['executables'] = queryA['executables'].replace(to_replace='/.*/', value='', regex=True)
      equiv_patternA = sw_mapping()
      for entry in equiv_patternA:
        queryA.loc[queryA['executables'].str.contains(entry[0]), 'executables'] = entry[1]
    if args.jobs:
      df = queryA
      df['cpuhours'] = df['cpuhours'].divide(3600).round(2)
      df['nodehours'] = df['nodehours'].divide(3600).round(2)
      args.sort = 'date' if not args.sort else args.sort
    else:
      dg = queryA.groupby(['users', sw_key]) if args.username else queryA.groupby(sw_key)
      df = dg['cpuhours'].sum().divide(3600).round(2).to_frame()
      df['nodehours'] = dg['nodehours'].sum().divide(3600).round(2)
      df['jobs'] = dg['jobs'].nunique()
      if args.execpath:
        df['modules'] = dg['modules'].unique()
      if not args.username:
        df['users'] = dg['users'].nunique()
    args.sort = 'cpuhours' if not args.sort else args.sort

    #print(df.sort_values(by=args.sort, ascending=args.asc).head())
    self.__modA = list(df.sort_values(by=args.sort, ascending=args.asc).reset_index().T.to_dict().values())

    print("Build time: %.2fs" % float(time() - t0))
    print("=============\n")

  def report_by(self, args):
    resultA = []
    headerA, headerT, fmtT, orderT = ModuleFormat(args) if args.module else ExecRunFormat(args)
    hline  = list(map(lambda x: "-"*len(x), headerT))
    resultA.append(headerT)
    resultA.append(hline)

    modA = self.__modA
    num = min(int(args.num), len(modA))
    if args.log:
      resultA = []
      for i in range(num):
        resultA.append(modA[i])

      return resultA

    for i in range(num):
      entryT = modA[i]
      resultA.append(list(map(lambda x, y: x % entryT[y], fmtT, orderT)))
      if not args.module:
        if args.sw:
          resultA[-1].append(entryT['executables'])
        else:
          resultA[-1].append(entryT['executables'] + " %s" % entryT['modules'])

      if args.group:
        group = get_osc_group(entryT['users'])
        resultA[-1].insert(-1, group)

    statsA = {'num': len(modA),
             'cpuhours': sum([x['cpuhours'] for x in modA])}
    if not args.jobs:
        statsA['jobs'] = sum([x['jobs'] for x in modA])
    return [headerA, resultA, statsA]

  def to_parquet(self, args, startdate, enddate):
    connect = self.__conn
    query = self.__query
    db_path = self.__path + '/xalt/%s' % args.syshost
    db_name = queryA = None
    year0 = month0 = '0'
    print("\nData processing ....")
    print("=============")
    for i in range(len(startdate)):
      year, month = startdate[i].split('-')[0:2]
      if year0 != year or month0 != month:
        if isinstance(queryA, pd.DataFrame):
          print(queryA.info(verbose=False))
          t0 = time()
          queryA.to_parquet(db_path + '/' + db_name, engine='pyarrow')
          print("Writing to %s: %.2fs" % (db_name, float(time() - t0)))
        queryA = None
        year0 = year 
        month0 = month
        print("Importing data %s-%s@%s from xalt_run" % (year, month, args.syshost))
      db_name = '%04d%02d.pq' % (int(year), int(month))
      t0 = time()
      q = pd.read_sql(query, connect, params=(args.syshost, startdate[i], enddate[i]))
      print("Query time (%s - %s): %.2fs" % (startdate[i], enddate[i], float(time() - t0)))
      queryA = queryA.append(q, ignore_index=True) if isinstance(queryA, pd.DataFrame) else q

    if isinstance(queryA, pd.DataFrame):
      print(queryA.info(verbose=False))
      t0 = time()
      queryA.to_parquet(db_path + '/' + db_name, engine='pyarrow', compression='snappy')
      print("Writing to %s: %.2fs" % (db_name, float(time() - t0)))
    
    print("=============\n")

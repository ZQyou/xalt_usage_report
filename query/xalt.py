from .xalt_format import ExecRunFormat, ModuleFormat
from .xalt_sw_mapping import sw_mapping
from .util import get_user_group, get_job_account, get_heap_status
import pandas as pd
from re import match, compile, IGNORECASE
from time import time 
from glob import glob

database_path = '/fs/ess/PZS0710/database'

class Xalt:
  def __init__(self, connect):
    self.__modA  = []
    self.__conn = connect
    self.__query = """SELECT
    date, run_time, run_id,
    job_id      AS jobs,
    user        AS users, 
    account     AS accounts,
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
    #
    # Object and user searching
    #
    sw_key = 'executables'
    sql_re = args.sql.lower()
    sw_re = None 
    if sql_re != '%':
       sw_re = sql_re.replace('%','')
       sw_re = '^' + sw_re if sql_re[0] != '%' else sw_re
       sw_re = sw_re + '$' if sql_re[-1] != '%' else sw_re
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

    if args.mpi:
      query += ' AND num_cores > 1 '

    if args.gpu:
      query += ' AND num_gpus > 0 '

    #print(query)

    db_path = self.__path + '/xalt/%s' % args.syshost
    db_list = [ f.split('/')[-1] for f in glob(db_path + '/*.pq', recursive=False) ]
    db_list.sort()

    connect = self.__conn
    queryA = df = q = None
    year0 = month0 = 0

    print("\nData processing ....")
    print("=============")
    for i in range(len(startdate)):
      year, month = list(map(int, startdate[i].split('-')[0:2]))
      # Check if we need to load new db for other months
      if year0 != year or month0 != month:
        df = None ; year0 = year ; month0 = month

      db_name = '%04d%02d' % (year, month)
      t0 = time()
      with_db = False if args.nopq else (len(glob(db_path + '/' + db_name + '*.pq', recursive=False)) > 0)
      if with_db:
        if not isinstance(df, pd.DataFrame): 
          # Read daily data, e.g. 20210328.pq
          db_re = compile(db_name + '\d+\.pq') 
          db_count = 0
          for db in db_list:
            db_m = match(db_re, db)
            if db_m:
              #print("Loading %s" % db_m.group(0))
              db_count += 1
              q = pd.read_parquet(db_path + '/' + db_m.group(0), engine='pyarrow')
              df = df.append(q, ignore_index=True) if isinstance(df, pd.DataFrame) else q

          if db_count > 0:
            print("Loading time for %d files: %.2fs" % (db_count, float(time() - t0)))
          else:
            print("Loading %s" % db_name + '.pq')
            df = pd.read_parquet(db_path + '/' + db_name + '.pq', engine='pyarrow')

        # Filtering
        t0 = time()
        criteria  = (df['date'] >= startdate[i]) & (df['date'] <= enddate[i])
        criteria &= (df['jobs'] != 'unknown')
        if sw_re:
          print("Searching for %s" % sw_re)
          criteria &= (df[sw_key].str.contains("(?i)%s" % sw_re)) 

        if args.user:
          criteria &= (df['users'] == args.user)

        if args.mpi:
          criteria &= (df['n_nodes'] > 1)

        if args.gpu:
          criteria &= (df['n_gpus'] > 0)

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
      if args.account:
        accounts = get_job_account(queryA['jobs'])
        queryA['jobs'] = accounts 

      groupA = []
      if args.account:
        groupA += ['jobs']

      if args.username:
        groupA += ['users']
      
      groupA += [sw_key]
      dg = queryA.groupby(groupA)

      df = dg['cpuhours'].sum().divide(3600).round(2).to_frame()
      df['nodehours'] = dg['nodehours'].sum().divide(3600).round(2)
      if not args.account:
        df['jobs'] = dg['jobs'].size() if args.count else dg['jobs'].nunique()

      if args.execpath:
        df['modules'] = dg['modules'].unique()

      if not args.username:
        df['users'] = dg['users'].nunique()

    args.sort = 'cpuhours' if not args.sort else args.sort

    #print(df.sort_values(by=args.sort, ascending=args.asc).head())
    self.__modA = list(df.sort_values(by=args.sort, ascending=args.asc).reset_index().T.to_dict().values())

    print("Build time: %.2fs" % float(time() - t0))
    print("=============\n")

    get_heap_status()

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
        group = get_user_group(entryT['users'])
        resultA[-1].insert(-1, group)

    statsA = {'num': len(modA),
             'cpuhours': sum([x['cpuhours'] for x in modA])}
    if not args.jobs and not args.account:
        statsA['jobs'] = sum([x['jobs'] for x in modA])

    return [headerA, resultA, statsA]

  def to_parquet(self, args, startdate, enddate):
    connect = self.__conn
    query = self.__query
    db_path = self.__path + '/xalt/%s' % args.syshost
    db_name = queryA = None
    print("\nData processing ....")
    print("=============")
    print("Importing data from xalt_run")
    for i in range(len(startdate)):
      t0 = time()
      ymd = list(map(int, startdate[i].split(' ')[0].split('-')))
      queryA = None
      db_name = '%04d%02d%02d.pq' % tuple(ymd)
      queryA = pd.read_sql(query, connect, params=(args.syshost, startdate[i], enddate[i]))
      print("Query time (%s - %s): %.2fs" % (startdate[i], enddate[i], float(time() - t0)))
      if isinstance(queryA, pd.DataFrame):
        print(queryA.info(verbose=False))
        t0 = time()
        queryA.to_parquet(db_path + '/' + db_name, engine='pyarrow', compression='snappy')
        print("Writing to %s: %.2fs" % (db_name, float(time() - t0)))
    
    print("=============\n")

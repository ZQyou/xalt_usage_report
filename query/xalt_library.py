from .xalt_format import LibraryFormat
from .util import get_heap_status
import os
import pandas as pd
from time import time 
from glob import glob
from re import compile, match

class Library:
  def __init__(self, connect):
    self.__modA  = []
    self.__conn = connect
    self.__query = """SELECT
    t3.date, t3.run_time,
    t3.job_id AS jobs,
    t3.user AS users,
    t3.num_gpus    AS n_gpus,
    t3.num_cores   AS n_cores,
    t3.num_threads AS n_thds,
    t3.num_nodes   AS n_nodes,
    t1.object_path AS libpaths,
    t1.module_name AS modules
    FROM xalt_object as t1, join_run_object as t2, xalt_run as t3 
    WHERE t3.syshost LIKE %s
    AND t1.obj_id = t2.obj_id AND t2.run_id = t3.run_id
    AND t3.date >= %s and t3.date <= %s
    """
    self.__path = usage_conf().get('Parquet', 'database_prefix')

  def build(self, args, startdate, enddate):
    sql_re = args.sql.lower()
    sw_re = None 
    if sql_re != '%':
       sw_re = sql_re.replace('%','')
       sw_re = '^' + sw_re if sql_re[0] != '%' else sw_re
       sw_re = sw_re + '$' if sql_re[-1] != '%' else sw_re

    query = self.__query
    query += ' AND LOWER(t1.module_name) LIKE %s ' 

    db_path = os.path.join(self.__path, '%s', % args.syshost, 'lib')
    db_list = [ f.split('/')[-1] for f in glob(db_path + '/*.pq', recursive=False) ]
    db_list.sort()

    connect = self.__conn
    queryA = df = q = None
    year0 = month0 = '0'

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
        if sw_re:
          print("Searching for %s" % sw_re)
          criteria = (df['date'] >= startdate[i]) & (df['date'] <= enddate[i]) & \
                     (df['modules'].str.contains("(?i)%s" % sw_re)) 
        else:
          criteria = (df['date'] >= startdate[i]) & (df['date'] <= enddate[i])

        # Avoid 'SettingWithCopyWarning'
        # https://maxpowerwastaken.github.io/blog/pandas_view_vs_copy/
        q = df.loc[df[criteria].index, :]
      else:
        print("Importing from xalt_object, join_run_object and xalt_run")
        t0 = time()
        q = pd.read_sql(query, connect,
                        params=(args.syshost, startdate[i], enddate[i], sql_re))

      print("Query time (%s - %s): %.2fs" % (startdate[i], enddate[i], float(time() - t0)))
      queryA = queryA.append(q, ignore_index=True) if isinstance(queryA, pd.DataFrame) else q

    queryA['cpuhours'] = queryA['run_time'] * queryA['n_thds'] * queryA['n_cores']
    queryA['nodehours'] = queryA['run_time'] * queryA['n_nodes']

    print(queryA.info(verbose=False))

    t0 = time()
    dg = queryA.groupby(['users', 'modules']) if args.username else queryA.groupby('modules')
    #df = dg.size().to_frame('n_libs')
    df = dg['cpuhours'].sum().divide(3600).round(2).to_frame()
    df['nodehours'] = dg['nodehours'].sum().divide(3600).round(2)
    if args.count:
      df['jobs'] = dg['jobs'].size()
    else:    
      df['jobs'] = dg['jobs'].nunique()
    if not args.username:
      df['users'] = dg['users'].nunique()
    df['libpaths'] = dg['libpaths'].unique()
#   df['n_libs'] = dg['libpaths'].count().to_frame('n_libs')

    args.sort = 'cpuhours' if not args.sort else args.sort
    #print(df.sort_values(by='n_libs', ascending=args.asc).head())
    self.__modA = list(df.sort_values(by=args.sort, ascending=args.asc).reset_index().T.to_dict().values())

    print("Build time: %.2fs" % float(time() - t0))
    print("=============\n")

    get_heap_status()

  def report_by(self, args):
    resultA = []
    headerA, headerT, fmtT, orderT = LibraryFormat(args)
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
      resultA[-1].append(entryT['modules'])
      resultA[-1].append('[%s ...]' % entryT['libpaths'][0].strip())

    statsA = {'num': len(modA)}
    return [headerA, resultA, statsA]

  def to_parquet(self, args, startdate, enddate):
    connect = self.__conn
    query = self.__query
    db_path = os.path.join(self.__path, '%s' % args.syshost, 'lib')
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

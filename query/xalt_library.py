from .xalt_format import LibraryFormat
import pandas as pd
from time import time 
from glob import glob

database_path = '/fs/ess/PZS0710/database'

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
    self.__path = database_path

  def build(self, args, startdate, enddate):
    sql_re = args.sql.lower()
    sw_re = None 
    if sql_re != '%':
       sw_re = sql_re.replace('%','')
       sw_re = '^' + sw_re if sql_re[0] != '%' else sw_re
       sw_re = sw_re + '$' if sql_re[-1] != '%' else sw_re

    query = self.__query
    query += ' AND LOWER(t1.module_name) LIKE %s ' 

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
      db_name = '%04d%02d' % (int(year), int(month)) + '_lib.pq'
      t0 = time()
      with_db = False if args.nopq else (db_name in db_list)
      if with_db:
        if not isinstance(df, pd.DataFrame): 
          print("Loading %s" % db_name)
          df = pd.read_parquet(db_path + '/' + db_name, engine='pyarrow')

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
        print("Importing data %s-%s@%s from xalt_object, join_run_object and xalt_run" % (year, month, args.syshost))
      db_name = '%04d%02d_lib.pq' % (int(year), int(month))
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

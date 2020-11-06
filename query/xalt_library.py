import pandas as pd
from time import time 

database_path = '/fs/ess/PZS0710/database'

def LibraryFormat(args):
  top_thing = "libraries"
  headerA = "\nTop %s %s sorted by n_libs\n" % (str(args.num), top_thing)
  headerT = ["# Libs", "Modules", "LibPath"]
  fmtT    = ["%d", "%s"]
  orderT  = ['n_libs', 'modules']

  headerA += '\n* Host: %s\n' % args.syshost
  if args.sql != '%':
    headerA += '* Search pattern: %s\n' % args.sql

  return [headerA, headerT, fmtT, orderT]

class Library:
  def __init__(self, connect):
    self.__modA  = []
    self.__conn = connect
    self.__query = """SELECT
    timestamp, 
    obj_id,
    object_path AS libpaths,
    module_name AS modules
    FROM xalt_object WHERE syshost LIKE %s
    AND timestamp >= %s and timestamp <= %s
    """
    self.__path = database_path

  def build(self, args, startdate, enddate):
    sql_re = args.sql.lower()
    query = self.__query
    query += ' AND LOWER(module_name) LIKE %s ' 

    connect = self.__conn
    queryA = df = q = None

    print("\nData processing ....")
    print("=============")
    for i in range(len(startdate)):
      print("Importing from xalt_object")
      t0 = time()
      q = pd.read_sql(query, connect,
            params=(args.syshost, startdate[i], enddate[i], sql_re))
      print("Query time (%s - %s): %.2fs" % (startdate[i], enddate[i], float(time() - t0)))

      queryA = queryA.append(q, ignore_index=True) if isinstance(queryA, pd.DataFrame) else q

    print(queryA.info(verbose=False))

    t0 = time()
    dg = queryA.groupby('modules')
    df = dg.size().to_frame('n_libs')
    df['libpaths'] = dg['libpaths'].unique()
#   df['n_libs'] = dg['libpaths'].count().to_frame('n_libs')

    #print(df.sort_values(by='n_libs', ascending=args.asc).head())
    self.__modA = list(df.sort_values(by='n_libs', ascending=args.asc).reset_index().T.to_dict().values())

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
        print("Importing data %s-%s@%s from xalt_object" % (year, month, args.syshost))
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

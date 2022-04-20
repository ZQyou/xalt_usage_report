import pandas as pd
import sys, os
from time import time
import numpy as np
from glob import glob

prefix = "/fs/ess/PZS0710/database/xalt"

def read_pq(system, database):
    t0 = time()
    #df = pd.read_parquet('test.parquet', engine='fastparquet')
    df = pd.read_parquet(os.path.join(prefix, system, database) + '.pq', engine='pyarrow')
    print("Load time: %.2fs" % float(time() - t0))
    print(df.head())
    print(df[df['run_time'] < 900].head())
    df.loc[df['run_time'] < 900, 'run_time'] *= 1/0.0001
    print(df.head())
#   print(df.size)
    print(len(df.index))
    print(df.columns)
    print(df.sort_values(by='run_id').iloc[[0,-1]])
#   print(df.iloc[[0,-1]])
'''    
    startdate = '2020-01-01 00:00:00'
    enddate = '2020-01-02 23:59:59'
    df = df[(df['date'] >= startdate) & (df['date'] <= enddate)]
    q = df['run_time'] * df['n_thds'] * df['n_cores']
    df['cpuhours'] = ""
    df.loc[df.index, 'cpuhours'] = q
    print(df.loc[df.index, 'cpuhours'].head())
    #print(df.tail(10))
    print(df.size/len(df.columns))
'''    

def main():
    if len(sys.argv[1:]) < 2:
        print("\nUsage:", sys.argv[0], "system database_name\n")
        sys.exit(2)

    system, database = sys.argv[1:3]

    db_list = [ f.split('/')[-1] for f in glob(prefix + '/%s' % system + '/%s*.pq' % database, recursive=False) ]
    print(db_list)

    read_pq(system, database)

    # https://coderzcolumn.com/tutorials/python/guppy-heapy-profile-memory-usage-in-python
    #from guppy import hpy
    #heap = hpy()
    #heap_status = heap.heap()
    #print("Heap Size : ", heap_status.size, " bytes\n")
    #print(heap_status)

if __name__ == '__main__':
    main()

import pandas as pd
import sys, os
from time import time
import numpy as np

prefix = "/fs/ess/PZS0710/database/xalt"

def read_pq(system, database):
    t0 = time()
    #df = pd.read_parquet('test.parquet', engine='fastparquet')
    df = pd.read_parquet(os.path.join(prefix, system, database) + '.pq', engine='pyarrow')
    print("Load time: %.2fs" % float(time() - t0))
    print(df.head())
    print(df.size)
    print(df.columns)
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
        print("\nUsage:", sys.argv[0], "system database\n")
        sys.exit(2)

    system, database = sys.argv[1:3]
    read_pq(system, database)

if __name__ == '__main__':
    main()

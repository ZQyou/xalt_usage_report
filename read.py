import pandas as pd
from time import time
import numpy as np

t0 = time()
#df = pd.read_parquet('test.parquet', engine='fastparquet')
df = pd.read_parquet('/fs/scratch/PZS0710/zyou/tmp/202001_xalt_owens.pq', engine='pyarrow')
print("Load time: %.2fs" % float(time() - t0))
print(df.head())
print(df.size)
print(df.columns)

startdate = '2020-01-01 00:00:00'
enddate = '2020-01-02 23:59:59'
df = df[(df['date'] >= startdate) & (df['date'] <= enddate)]
q = df['run_time'] * df['n_thds'] * df['n_cores']
df['cpuhours'] = ""
df.loc[df.index, 'cpuhours'] = q
print(df.loc[df.index, 'cpuhours'].head())
#print(df.tail(10))
print(df.size/len(df.columns))

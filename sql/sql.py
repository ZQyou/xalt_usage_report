from .conf import xalt_conf, pbsacct_conf
from base64 import b64decode
import pymysql, base64
from pprint import pprint

class Sql(object):
  def __init__(self, args, db=None):
     self.args = args
     self.config = pbsacct_conf(args.syshost, args.confFn) \
             if db == 'pbsacct' else xalt_conf(args.confFn)
     self.conn = None
     self.cursor = None
     self.dbconf = db if db == 'pbsacct' else 'MYSQL'
     self.db = db if db == 'pbsacct' else self.config.get(self.dbconf,"DB")

  def connect(self):
    config = self.config
    self.conn = pymysql.connect(
        host=config.get(self.dbconf,"HOST"), \
        user=config.get(self.dbconf,"USER"), \
        password=b64decode(config.get(self.dbconf,"PASSWD")), \
        database=self.db)
    self.cursor = self.conn.cursor()

  def describe_table(self):
    query = "DESCRIBE %s" % self.args.show
    self.cursor.execute(query)
    resultA = self.cursor.fetchall()
    return resultA

  def show_tables(self):
    query = "SHOW TABLES"
    self.cursor.execute(query)
    resultA = self.cursor.fetchall()
    return resultA

  def user_query(self):
    query = self.args.query
    self.cursor.execute(query)
    header = [i[0] for i in self.cursor.description]
    hline  = list(map(lambda x: "-"*len(x), header))
    resultA = list(self.cursor.fetchall())
    resultA.insert(0, hline)
    resultA.insert(0, header)
    return resultA
  
  def truncate(self):
      # set FOREIGN_KEY_CHECKS=0; truncate table xalt_run; set FOREIGN_KEY_CHECKS=1'
      if self.db == 'xalt_owens' or self.db == 'xalt_pitzer':
        print("Cannot trucate tables of XALT production database")
        return
      self.cursor.execute('show tables')
      resultA = list(self.cursor.fetchall())
      self.cursor.execute('set FOREIGN_KEY_CHECKS=0')
      for t in resultA:
        print('Truncating %s' % t)
        self.cursor.execute('truncate table %s' % t)

      self.cursor.execute('set FOREIGN_KEY_CHECKS=1')
      return

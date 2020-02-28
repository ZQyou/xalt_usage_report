from .conf import xalt_conf, pbsacct_conf
from base64 import b64decode
import pymysql, base64

class Sql(object):
  def __init__(self, args, db=None):
     self.args = args
     self.config = pbsacct_conf(args.syshost, args.confFn) \
             if db == 'pbsacct' else xalt_conf(args.confFn)
     self.conn = None
     self.cursor = None
     self.db = db if db == 'pbsacct' else 'xalt_%s' % args.syshost
     self.dbconf = db if db == 'pbsacct' else 'MYSQL'

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
    query = self.args.dbg 
#   query = args.dbg + \
#   """
#   ORDER BY date DESC
#   """ + \
#   "LIMIT " + str(args.num)
    self.cursor.execute(query)
    resultA = self.cursor.fetchall()
    return resultA

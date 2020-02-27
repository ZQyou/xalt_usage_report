from .conf import xalt_conf, pbsacct_conf
from base64 import b64decode
import pymysql, base64

class Sql(object):
  def __init__(self, args):
     self.args = args
     self.config = xalt_conf(args.confFn)
     self.conn = None
     self.cursor = None

  def connect(self):
    config = self.config
    self.conn = pymysql.connect(
        host=config.get("MYSQL","HOST"), \
        user=config.get("MYSQL","USER"), \
        password=b64decode(config.get("MYSQL","PASSWD")), \
        database="xalt_%s" % self.args.syshost)
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

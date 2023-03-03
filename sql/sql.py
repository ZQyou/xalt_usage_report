from .conf import xalt_conf
from base64 import b64decode
import pymysql, base64
from pprint import pprint

class Sql(object):
  def __init__(self, args):
     self.args = args
     self.config = xalt_conf(args.confFn)
     self.conn = None
     self.cursor = None
     self.dbconf = 'MYSQL'
     self.db = self.config.get(self.dbconf,"DB")
     if args.db:
         self.db = args.db

     print("Database: %s" % self.db)

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

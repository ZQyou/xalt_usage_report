import os, argparse
try:
  import configparser
except:
  import ConfigParser as configparser

syshost = os.environ.get("LMOD_SYSTEM_NAME", "%")

class CmdLineOptions(object):
  """ Command line Options class """

  def __init__(self):
    """ Empty Ctor """
    pass
  
  def execute(self):
    """ Specify command line arguments and parse the command line"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--confFn",   dest='confFn',    action="store",       default = None,           help="full path to database conf")
    parser.add_argument("--start",    dest='startD',    action="store",       default = None,           help="start date, e.g. 2018-10-23")
    parser.add_argument("--end",      dest='endD',      action="store",       default = None,           help="end date")
    parser.add_argument("--syshost",  dest='syshost',   action="store",       default = syshost,        help="syshost")
    parser.add_argument("--db",       dest='db',        action="store",       default = None,           help="xalt database")
    parser.add_argument("--sw",       dest='sw',        action="store_true",  default = True,           help="print software/executable usage (default)")
    parser.add_argument("--count",    dest='count',     action="store_true",                            help="count entries instead of unique execpaths")
    parser.add_argument("--nopq",     dest='nopq',      action="store_true",                            help="do not use local parquet database")
    parser.add_argument("--topq",     dest='topq',      action="store_true",                            help="write query result to parquet")
    parser.add_argument("--pq_path",  dest='pq_path',   action="store",       default = "default",      help="overwrite the default pq-files path")
    parser.add_argument("--module",   dest='module',    action="store_true",                            help="print module usage")
    parser.add_argument("--execpath", dest='execpath',  action="store_true",                            help="print executable paths; this is break-down of --sw mode")
    parser.add_argument("--library",  dest='library',   action="store_true",                            help="print library usage")
    parser.add_argument("--sql",      dest='sql',       action="store",       default = "%",            help="SQL pattern for matching modules or executables; '%%' is SQL wildcard character")
    parser.add_argument("--num",      dest='num',       action="store",       default = 20,             help="top number of entries to report")
    parser.add_argument("--sort",     dest='sort',      action="store",       default = None,           help="sort the result by cpuhours (default) | users | jobs | date")
    parser.add_argument("--asc",      dest='asc',       action="store_true",                            help="ascending sort")
    parser.add_argument("--username", dest='username',  action="store_true",                            help="print username instead of # users")
    parser.add_argument("--group",    dest='group',     action="store_true",                            help="print username and primary group")
    parser.add_argument("--account",  dest='account',   action="store_true",                            help="print job accounts")
    parser.add_argument("--gpu",      dest='gpu',       action="store_true",                            help="print usage for GPU jobs")
    parser.add_argument("--mpi",      dest='mpi',       action="store_true",                            help="print usage for parallel jobs")
    parser.add_argument("--user",     dest='user',      action="store",       default = None,           help="user account for matching")
    parser.add_argument("--project",  dest='project',   action="store",       default = None,           help="project account for matching")
    parser.add_argument("--jobs",     dest='jobs',      action="store_true",                            help="print job ids and dates")
    parser.add_argument("--csv",      dest='csv',       action="store_true",                            help="print in CSV format")
    parser.add_argument("--query",    dest='query',     action="store",       default = None,           help="custom user query")
    parser.add_argument("--truncate", dest='trucate',   action="store_true",  default = False,          help="trucate all tables of test database")
    parser.add_argument("--log",      dest='log',       action="store",       default = None,           help="dump the result to log: stdout | syslog")
    parser.add_argument("--show",     dest='show',      action="store",       default = None,           help="show/describe tables of thea database, e.g. --show tables")
    parser.add_argument("--report",   dest='report',    action="store_true",                            help="report from original xalt_usage_report.py")
    parser.add_argument("--full",     dest='full',      action="store_true",                            help="report core hours by compiler")
    parser.add_argument("--days",     dest='days',      action="store",       default = 7,              help="report from now to DAYS back")
    args = parser.parse_args()
    return args

def xalt_conf(confFn):
  if not confFn:
    confFn = os.path.join(os.environ.get("XALT_ETC_DIR","./"), "xalt_db.conf")

  print("XALT Database Config: %s" % confFn)
  config = configparser.ConfigParser()     
  config.read(confFn)
  return config

def usage_conf(confFn=None):
  if not confFn:
    confFn = os.path.join(os.path.dirname(os.path.realpath(__file__)), "usage.conf")
      
  print("Usage Tool Config: %s" % confFn)
  config = configparser.ConfigParser()     
  config.read(confFn)
  return config

if __name__ == '__main__': 
  syshost = os.environ.get("LMOD_SYSTEM_NAME", "%")

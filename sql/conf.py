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
    parser.add_argument("--confFn",  dest='confFn',    action="store",       default = "xalt_db.conf", help="db name")
    parser.add_argument("--start",   dest='startD',    action="store",       default = None,           help="start date, e.g 2018-10-23")
    parser.add_argument("--end",     dest='endD',      action="store",       default = None,           help="end date")
    parser.add_argument("--syshost", dest='syshost',   action="store",       default = syshost,        help="syshost")
    parser.add_argument("--sw",      dest='sw',        action="store_true",  default = True,           help="print software/executable usage (default)")
    parser.add_argument("--count",   dest='count',     action="store_true",                            help="count entries instead of unique execpaths")
    parser.add_argument("--nopq",    dest='nopq',      action="store_true",                            help="do not use local parquet database")
    parser.add_argument("--topq",    dest='topq',      action="store_true",                            help="write query result to parquet")
    parser.add_argument("--module",  dest='module',    action="store_true",                            help="print module usage")
    parser.add_argument("--execpath",dest='execpath',  action="store_true",                            help="print executable paths; this is break-down of --sw mode")
    parser.add_argument("--library", dest='library',   action="store_true",                            help="print library usage")
    parser.add_argument("--sql",     dest='sql',       action="store",       default = "%",            help="SQL pattern for matching modules or executables; '%%' is SQL wildcard character")
    parser.add_argument("--num",     dest='num',       action="store",       default = 20,             help="top number of entries to report")
    parser.add_argument("--sort",    dest='sort',      action="store",       default = None,           help="sort the result by cpuhours (default) | users | jobs | date")
    parser.add_argument("--asc",     dest='asc',       action="store_true",                            help="ascending sort")
    parser.add_argument("--username",dest='username',  action="store_true",                            help="print user accounts instead of # users")
    parser.add_argument("--group",   dest='group',     action="store_true",                            help="print user accounts and groups")
    parser.add_argument("--gpu",     dest='gpu',       action="store_true",                            help="print GPU usage")
    parser.add_argument("--mpi",     dest='mpi',       action="store_true",                            help="print MPI jobs (num_cores > 1)")
    parser.add_argument("--user",    dest='user',      action="store",       default = None,           help="user account for matching")
    parser.add_argument("--jobs",    dest='jobs',      action="store_true",                            help="print job ids and dates")
    parser.add_argument("--csv",     dest='csv',       action="store_true",                            help="print in CSV format")
    parser.add_argument("--query",   dest='query',     action="store",       default = None,           help="custom user query")
    parser.add_argument("--log",     dest='log',       action="store",       default = None,           help="dump the result to log: stdout | syslog")
    parser.add_argument("--show",    dest='show',      action="store",       default = None,           help="show/describe tables of thea database, e.g. --show tables")
    parser.add_argument("--report",  dest='report',    action="store_true",                            help="report from original xalt_usage_report.py")
    parser.add_argument("--full",    dest='full',      action="store_true",                            help="report core hours by compiler")
    parser.add_argument("--days",    dest='days',      action="store",       default = 7,              help="report from now to DAYS back")
    args = parser.parse_args()
    return args

def xalt_conf(confFn):
  XALT_ETC_DIR = os.environ.get("XALT_ETC_DIR","./")
  config       = configparser.ConfigParser()     
  configFn     = os.path.join(XALT_ETC_DIR, confFn)
  config.read(configFn)

  return config

def pbsacct_conf(syshost, confFn):
  config  = configparser.ConfigParser()     
  if not confFn:
    script_path = os.path.dirname(os.path.realpath(__file__))
    confFn = os.path.join(script_path,"..","conf",".db_conf")
    if not os.path.isfile(confFn):
      if syshost == "pitzer":
        confFn = os.path.join("/apps/software_usage","conf",".db_conf")
      elif syshost == "owens":
        confFn = os.path.join("/usr/local/software_usage","conf",".db_conf")
  #print("Loading the config file", confFn)
  config.read(confFn)

  return config

if __name__ == '__main__': 
  syshost = os.environ.get("LMOD_SYSTEM_NAME", "%")
  config = pbsacct_conf(syshost, None)
  print(config.get("pbsacct","HOST"))

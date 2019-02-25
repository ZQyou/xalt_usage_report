from __future__ import print_function
import os
try:
  import configparser
except:
  import ConfigParser as configparser


def pbsacct_conf(syshost, confFn):
  PBSTOOLS_DIR = os.environ.get("PBSTOOLS_DIR","./")
  if not os.environ.has_key("PBSTOOLS_DIR"): 
     PBSTOOLS_DIR = "/usr/local"
  PBSTOOLS_ETC_DIR = os.path.join(PBSTOOLS_DIR, "etc")

  config   = configparser.ConfigParser()     
  configFn = os.path.join(PBSTOOLS_ETC_DIR, confFn)
  if syshost == 'pitzer':
    script_path = os.path.dirname(os.path.realpath(__file__))
    configFn = os.path.join(script_path,"..","conf",".db_conf")
  else:
    configFn = os.path.join(os.environ.get("HOME"),".db_conf")
  config.read(configFn)

  return config

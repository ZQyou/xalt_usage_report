from __future__ import print_function
import os
try:
  import configparser
except:
  import ConfigParser as configparser

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

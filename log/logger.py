import logging
import logging.handlers
import sys

class LoggerAdapter(logging.LoggerAdapter):
  def __init__(self, logger=None, database=None, syshost=None):
    super(LoggerAdapter, self).__init__(
      logger,
      {
        'database': database,
        'syshost': syshost,
#       'year': None,
#       'month': None,
        'software': None,
        'executables': None,
        'cpuhours': 0,
        'corehours': 0,
        'nodehours': 0,
        'jobs': 0,
        'users': 0,
        'groups': 0,
        'accounts': 0,
      }
    )

  def log_usage(self, level, extra, msg=None):
    for key in extra:
      self.extra[key] = extra[key]
    self.log(level, msg)

def syslog_logging(database, syshost, extra=[], handler='stdout'):
  fmt = (
#   'sw_usage_dev2: '
    'syshost=%(syshost)s '
    'database=%(database)s '
#   'year=%(year)s '
#   'month=%(month)s '
    'software=%(software)s '
    'executables=%(executables)s '
    'cpuhours=%(cpuhours)s '
    'corehours=%(corehours)s '
    'nodehours=%(nodehours)s '
    'n_jobs=%(jobs)s '
    'n_users=%(users)s '
    'n_groups=%(groups)s '
    'n_accounts=%(accounts)s '
  )
  datefmt = '%FT%T'

  _logger = logging.getLogger(__name__)
  syslog = None
  if handler == 'stdout':
    syslog = logging.StreamHandler(stream=sys.stdout)
  elif handler == 'syslog':
    raise Exception("syslog_logging: handler type %s is disabled" % handler)
    #syslog = logging.handlers.SysLogHandler(address='/dev/log')
  else:
    raise Exception("syslog_logging: unsupported handler type %s" % handler)

  syslog.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
  _logger.addHandler(syslog)
  adapter = LoggerAdapter(_logger, syshost, database)
  _logger.setLevel(logging.INFO)
  for x in extra:
    adapter.log_usage(logging.INFO, x)


""" logging adapter example """
""" https://stackoverflow.com/questions/39467271/cant-get-this-custom-logging-adapter-example-to-work """
"""
class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # use my_context from kwargs or the default given on instantiation
        my_context = kwargs.pop('my_context', self.extra['my_context'])
        return '[%s] %s' % (my_context, msg), kwargs

logger = logging.getLogger(__name__)
syslog = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(message)s')
syslog.setFormatter(formatter)
logger.addHandler(syslog)
adapter = CustomAdapter(logger, {'my_context': '1956'})
logger.setLevel(logging.INFO)

adapter.info('The sky is so blue', my_context='6642')
adapter.info('The sky is so blue')
"""


if __name__ == '__main__': 
  syshost = 'pitzer'
  database = 'pbsacct'
  handler = 'stdout'
  extra = [{
#   'year': 2018,
#   'month': 11,
    'software': 'vasp',
    'executables': 'VASP',
    'cpuhours':  267944,
    'corehours': 256244,
    'nodehours': 8379,
    'jobs': 594
  }]
  syslog_logging(database, syshost, extra, handler)

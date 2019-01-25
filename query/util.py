from subprocess import Popen, PIPE
from re import search, compile, IGNORECASE

group_re = compile('Primary Group:\s+(.*)\n', IGNORECASE)
def get_osc_group(username):
  process = Popen(['OSCfinger %s' % username], shell=True, stdout=PIPE, stderr=PIPE)
  stdout, stderr = process.communicate()
  return search(group_re, stdout).group(1)


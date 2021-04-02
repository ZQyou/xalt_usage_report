def ExecRunFormat(args):
  top_msg = "software/executables" if args.sw else "executable paths"
  count = "# Counts" if args.count else "# Jobs"
  headerA = "\nTop %s %s sorted by %s\n" % (str(args.num), top_msg, args.sort)
  headerT = ["CPUHrs", "NodeHrs", "%s" % count, "# Users"]
  fmtT    = ["%.2f", "%.2f", "%d", "%d" ]
  orderT  = ['cpuhours', 'nodehours', 'jobs', 'users']
  if args.username:
    headerA = "\nTop %s %s used in username\n" % (str(args.num), top_msg)
    headerT = ["CPUHrs", "NodeHrs", "%s" % count, "Username"]
    fmtT    = ["%.2f", "%.2f", "%d", "%s"]
    orderT  = ['cpuhours', 'nodehours', 'jobs', 'users']
    if args.group:
       headerT.insert(-1, "Group")

  if args.account:
    headerA = "\nTop %s %s used in account\n" % (str(args.num), top_msg)
    headerT = ["CPUHrs", "NodeHrs", "Account", "Users"]
    fmtT    = ["%.2f", "%.2f", "%s", "%d"]
    orderT  = ['cpuhours', 'nodehours', 'jobs', 'users']

  if args.account and args.username:
    headerA = "\nTop %s %s used in account and username\n" % (str(args.num), top_msg)
    headerT = ["CPUHrs", "NodeHrs", "Account", "Username"]
    fmtT    = ["%.2f", "%.2f", "%s", "%s"]
    orderT  = ['cpuhours', 'nodehours', 'jobs', 'users']

  if args.user:
    headerA = "\nTop %s %s used by %s\n" % (str(args.num), top_msg, args.user)
    headerT = ["CPUHrs", "NodeHrs", "%s" % count]
    fmtT    = ["%.2f", "%.2f", "%d"]
    orderT  = ['cpuhours', 'nodehours', 'jobs']

  if args.jobs:
    headerA = "\nFirst %s jobs sorted by %s\n" % (str(args.num), args.sort)
    if args.user:
      headerA = "\nFirst %s jobs used by %s\n" % (str(args.num), args.user)
    headerT = ["Date", "JobID", "CPUHrs", "NodeHrs", "# GPUs", "# Cores", "# Threads"]
    fmtT    = ["%s", "%s", "%.2f", "%.2f", "%d", "%d", "%d"]
    orderT  = ['date', 'jobs', 'cpuhours', 'nodehours', 'n_gpus', 'n_cores', 'n_thds']

  headerT += ["Software/Executable"] if args.sw else ["ExecPath"]

  headerA += '\n* Host: %s\n' % args.syshost
  if args.sql != '%':
    headerA += '* Search pattern: %s\n' % args.sql
  if args.gpu:
    headerA += '* GPU jobs only\n'
  headerA += '* WARNING: CPUHrs is executable walltime x # cores x # threads, not actual CPU utilization\n'
  headerA += '* WARNING: Runs without JobID are NOT counted\n'

  return [headerA, headerT, fmtT, orderT]

def ModuleFormat(args):
  headerA = "\nTop %s  modules sorted by %s\n" % (str(args.num), args.sort)
  headerT = ["CPUHrs", "NodeHrs", "# Jobs", "# Users", "Modules"]
  fmtT    = ["%.2f", "%.2f", "%d", "%d", "%s"]
  orderT  = ['cpuhours', 'nodehours', 'jobs', 'users', 'modules']
  if args.username:
    headerA = "\nTop %s modules used by users\n" % (str(args.num))
    headerT = ["CPUHrs", "NodeHrs", "# Jobs", "Username", "Modules"]
    fmtT    = ["%.2f", "%.2f", "%d", "%s", "%s"]
    orderT  = ['cpuhours', 'nodehours', 'jobs', 'users', 'modules']
    if args.group:
       headerT.insert(-1, "Group")

  if args.user:
    headerA = "\nTop %s modules used by %s\n" % (str(args.num), args.user)
    headerT = ["CPUHrs", "NodeHrs", "# Jobs", "Modules"]
    fmtT    = ["%.2f", "%2.f", "%d", "%s"]
    orderT  = ['cpuhours', 'nodehours', 'jobs', 'modules']

  if args.jobs:
    headerA = "\nFirst %s jobs sorted by %s\n" % (str(args.num), args.sort)
    if args.user:
      headerA = "\nFirst %s jobs used by %s\n" % (str(args.num), args.user)

    headerT = ["Date", "JobID", "CPUHrs", "NodeHrs", "# GPUs", "# Cores", "# Threads", "Modules"]
    fmtT    = ["%s", "%s", "%.2f", "%.2f", "%d", "%d", "%d", "%s"]
    orderT  = ['date', 'jobs', 'cpuhours', 'nodehours', 'n_gpus', 'n_cores', 'n_thds', 'modules']

  headerA += '\n* Host: %s\n' % args.syshost
  if args.sql != '%':
    headerA += '* Search pattern: %s\n' % args.sql

  if args.gpu:
    headerA += '* GPU jobs only\n'

  headerA += '* WARNING: CPUHrs is executable walltime x # cores x # threads, not actual CPU utilization\n'
  headerA += '* WARNING: Runs without JobID are NOT counted\n'

  return [headerA, headerT, fmtT, orderT]

def LibraryFormat(args):
  top_msg = "library modules"
  headerA = "\nTop %s %s sorted by %s\n" % (str(args.num), top_msg, args.sort)
  headerT = ["CPUHrs", "NodeHrs", "# Jobs", "# Users"]
  fmtT    = ["%.2f", "%.2f", "%d", "%d", "%s"]
  orderT  = ['cpuhours', 'nodehours', 'jobs', 'users']
  if args.username:
    headerA = "\nTop %s %s used by users\n" % (str(args.num), top_msg)
    headerT = ["CPUHrs", "NodeHrs", "# Jobs", "Username"]
    fmtT    = ["%.2f", "%.2f", "%d", "%s"]
    orderT  = ['cpuhours', 'nodehours', 'jobs', 'users']

  headerT += ["Library Module"]
  headerA += '\n* Host: %s\n' % args.syshost
  if args.sql != '%':
    headerA += '* Search pattern: %s\n' % args.sql

  return [headerA, headerT, fmtT, orderT]

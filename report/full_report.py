from __future__ import print_function
from .misc import * 
from .execrun import ExecRun, ExecRunLink
from .module import ModuleExec
from .compiler import CompilerUsageByCount, CompilerUsageByCoreHours
from .library import Libraries

from BeautifulTbl      import BeautifulTbl

def full_report(cursor, args, startdate, enddate):
  ############################################################
  #  Over all job counts
  resultA = kinds_of_jobs(cursor, args, startdate, enddate)
  bt      = BeautifulTbl(tbl=resultA, gap = 4, justify = "lrrrrrrr")
  print("----------------------")
  print("Overall MPI Job Counts")
  print("----------------------")
  print("")
  print(bt.build_tbl())
  print("\n")
  print("Where        usr: executables built by user")
  print("             sys: executables managed by system-level modulefiles")
  print("      usr-script: shell scripts in a user's account")
  print("      sys-script: shell scripts managed by system-level modulefiles")
  
  ############################################################
  #  Self-build vs. BuildU != RunU
  resultA = running_other_exec(cursor, args, startdate, enddate)
  bt      = BeautifulTbl(tbl=resultA, gap = 2, justify = "lrrr")
  print("")
  print("---------------------------------------------------")
  print("Comparing MPI Self-build vs. Build User != Run User")
  print("---------------------------------------------------")
  print("")
  print(bt.build_tbl())
  
  print("")
  print("-------------------")
  print("Top MPI Executables")
  print("-------------------")
  print("")
  
  ############################################################
  #  Build top executable list
  execA = ExecRun(cursor)
  execA.build(args, startdate, enddate)
  
  ############################################################
  #  Report of Top EXEC by Core Hours
  resultA, sumCH = execA.report_by(args,"corehours")
  bt             = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
  print("\nTop",args.num, "MPI Executables sorted by Core-hours (Total Core Hours(M):",sumCH*1.0e-6,")\n")
  print(bt.build_tbl())
  
  ############################################################
  #  Report of Top EXEC by Num Jobs
  resultA, sumCH  = execA.report_by(args,"n_jobs")
  bt              = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
  print("\nTop",args.num, "MPI Executables sorted by # Jobs\n")
  print(bt.build_tbl())
  
  ############################################################
  #  Report of Top EXEC by Users
  resultA, sumCH = execA.report_by(args,"n_users")
  bt             = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
  print("\nTop",args.num, "MPI Executables sorted by # Users\n")
  print(bt.build_tbl())
  
  if (args.full):
    ############################################################
    #  Report of Top EXEC by Corehours for gcc
    execA          = ExecRunLink(cursor)
    execA.build(args, startdate, enddate,"gcc")
    resultA, sumCH = execA.report_by(args,"corehours")
    bt             = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
    print("\nTop",args.num, "MPI Executables sorted by Core-hours for gcc\n")
    print(bt.build_tbl())

    ############################################################
    #  Report of Top EXEC by Corehours for g++
    execA          = ExecRunLink(cursor)
    execA.build(args, startdate, enddate,"g++")
    resultA, sumCH = execA.report_by(args,"corehours")
    bt             = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
    print("\nTop",args.num, "MPI Executables sorted by Core-hours for g++\n")
    print(bt.build_tbl())
  
    ############################################################
    #  Report of Top EXEC by Corehours for gfortran
    execA          = ExecRunLink(cursor)
    execA.build(args, startdate, enddate,"gfortran")
    resultA, sumCH = execA.report_by(args,"corehours")
    bt             = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
    print("\nTop",args.num, "MPI Executables sorted by Core-hours for gfortran\n")
    print(bt.build_tbl())

    ############################################################
    #  Report of Top EXEC by Corehours for ifort
    execA          = ExecRunLink(cursor)
    execA.build(args, startdate, enddate,"ifort")
    resultA, sumCH = execA.report_by(args,"corehours")
    bt             = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
    print("\nTop",args.num, "MPI Executables sorted by Core-hours for ifort\n")
    print(bt.build_tbl())

    ############################################################
    #  Report of Top EXEC by Corehours for icc
    execA          = ExecRunLink(cursor)
    execA.build(args, startdate, enddate,"icc")
    resultA, sumCH = execA.report_by(args,"corehours")
    bt             = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
    print("\nTop",args.num, "MPI Executables sorted by Core-hours for icc\n")
    print(bt.build_tbl())
  
    ############################################################
    #  Report of Top EXEC by Corehours for icpc
    execA          = ExecRunLink(cursor)
    execA.build(args, startdate, enddate,"icpc")
    resultA, sumCH = execA.report_by(args,"corehours")
    bt             = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
    print("\nTop",args.num, "MPI Executables sorted by Core-hours for icpc\n")
    print(bt.build_tbl())

  ############################################################
  #  Report of Top Modules by Core Hours
  modA = ModuleExec(cursor)
  modA.build(args, startdate, enddate)
  resultA = modA.report_by(args,"corehours")
  bt      = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
  print("\nTop",args.num, "MPI Modules sorted by Core-hours \n")
  print(bt.build_tbl())
  
  ############################################################
  # Report on Compiler (linker) usage by Count
  linkA = CompilerUsageByCount(cursor)
  linkA.build(args, startdate, enddate)
  resultA = linkA.report_by(args, "count")
  bt      = BeautifulTbl(tbl=resultA, gap = 2, justify = "rl")
  print("\nCompiler usage by Count\n")
  print(bt.build_tbl())
  
  ############################################################
  # Report on Compiler (linker) usage by Core Hours
  linkA = CompilerUsageByCoreHours(cursor)
  linkA.build(args, startdate, enddate)
  resultA = linkA.report_by(args, "corehours")
  bt      = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrl")
  print("\nCompiler usage by Corehours\n")
  print(bt.build_tbl())
  
  ############################################################
  #  Report of Library by short module name usage by Core Hours.
  libA = Libraries(cursor)
  libA.build(args, startdate, enddate)
  resultA = libA.group_report_by(args,"corehours")
  bt      = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrrl")
  print("")
  print("---------------------------------------------------------------------------------")
  print("Libraries used by MPI Executables sorted by Core Hours grouped by module families")
  print("---------------------------------------------------------------------------------")
  print("")
  print(bt.build_tbl())
  
  ############################################################
  #  Report of Library usage by Core Hours.
  resultA = libA.report_by(args,"corehours")
  bt      = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrrl")
  print("")
  print("------------------------------------------------------")
  print("Libraries used by MPI Executables sorted by Core Hours")
  print("------------------------------------------------------")
  print("")
  print(bt.build_tbl())
  
  ############################################################
  #  Report of Library usage by Core Hours for largemem.
  libA = Libraries(cursor)
  libA.build(args, startdate, enddate, "largemem")
  resultA = libA.report_by(args,"corehours")
  if (resultA):
    bt      = BeautifulTbl(tbl=resultA, gap = 2, justify = "rrrrl")
    print("")
    print("-------------------------------------------------------------------")
    print("Libraries used by MPI Executables sorted by Core Hours for largemem")
    print("-------------------------------------------------------------------")
    print("")
    print(bt.build_tbl())

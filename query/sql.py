def describe_table(cursor, args):
    query = "DESCRIBE %s" % args.show
    cursor.execute(query)
    resultA = cursor.fetchall()
    return resultA

def show_tables(cursor):
    query = "SHOW TABLES"
    cursor.execute(query)
    resultA = cursor.fetchall()
    return resultA

def xalt_select_data(cursor, args, startdate, enddate):
    query =  "SELECT "+ args.data + " FROM xalt_run" \
    """
    WHERE syshost like %s
    AND date >= %s and date <= %s
    ORDER BY date DESC
    """ + \
    "LIMIT " + str(args.num)
    print query
    cursor.execute(query, (args.syshost, startdate, enddate))
    resultA = cursor.fetchall()
    return resultA

def user_sql(cursor, args):
    query = args.dbg + \
    """
    ORDER BY date DESC
    """ + \
    "LIMIT " + str(args.num)

    cursor.execute(query)
    resultA = cursor.fetchall()
    return resultA

def pbsacct_select_jobs(cursor, args, startdate, enddate):
    query =  "SELECT "+ args.data + " FROM Jobs" + \
    " WHERE system like '%s' " % args.syshost + \
    " AND start_ts >= %s and start_ts <= %s" % (startdate, enddate) + \
    " ORDER BY start_ts DESC LIMIT " + str(args.num)
    print query
    cursor.execute(query)
    resultA = cursor.fetchall()
    return resultA

def describe_xalt_run(cursor):
    query = "DESCRIBE xalt_run"
    cursor.execute(query)
    resultA = cursor.fetchall()
    return resultA

def list_data(cursor, args, startdate, enddate):
    query =  "SELECT date," + args.data + \
    """
    FROM xalt_run
    WHERE syshost like %s
    AND date >= %s and date < %s
    ORDER BY date DESC
    """ + \
    "LIMIT " + str(args.num)

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

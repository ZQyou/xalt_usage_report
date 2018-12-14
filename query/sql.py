def describe_table(cursor, args):
    query = "DESCRIBE %s" % args.list
    cursor.execute(query)
    resultA = cursor.fetchall()
    return resultA

def show_tables(cursor):
    query = "SHOW TABLES"
    cursor.execute(query)
    resultA = cursor.fetchall()
    return resultA

def select_data(cursor, args, startdate, enddate):
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

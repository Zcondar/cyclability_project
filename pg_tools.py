import psycopg2
import csv

def pgconnect():
    try:
        # adding options properties to set the schema
        conn = psycopg2.connect(host='Cyclability.cquxnucnvz8p.ap-southeast-2.rds.amazonaws.com',
                                database='postgres',
                                user='team',
                                password='password',
                                options=f'-c search_path=cyclability')
        print('connected')
    except Exception as e:
        print("unable to connect to the database")
        print(e)
    return conn


def pgexec(conn, sqlcmd, args, msg, silent=False):
    retval = False
    with conn:
        with conn.cursor() as cur:
            try:
                if args is None:
                    cur.execute(sqlcmd)
                else:
                    cur.execute(sqlcmd, args)
                if silent == False:
                    print("success: " + msg)
                retval = True
            except Exception as e:
                if silent == False:
                    print("db error: ")
                    print(e)
    return retval


def pgquery(conn, sqlcmd, args, silent=False):
    """ utility function to execute some SQL query statement
        can take optional arguments to fill in (dictionary)
        will print out on screen the result set of the query
        error and transaction handling built-in """
    retval = False
    with conn:
        with conn.cursor() as cur:
            try:
                if args is None:
                    cur.execute(sqlcmd)
                else:
                    cur.execute(sqlcmd, args)
                if silent == False:
                    for record in cur:
                        print(record)
                retval = True
            except Exception as e:
                if silent == False:
                    print("db read error: ")
                    print(e)
    return retval


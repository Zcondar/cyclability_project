import psycopg2
import csv
import pprint

def pgconnect():
    try:
        conn = psycopg2.connect(host='Cyclability.cquxnucnvz8p.ap-southeast-2.rds.amazonaws.com',
                                database='postgres',
                                user='team',
                                password='password')

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

def clean_empty_string(data):
    for row in data:
        for key, value in row.items():
            if value == "":
                row[key] = None

business_stats_data = list(csv.DictReader(open('BusinessStats.csv')))
print(business_stats_data[-5])

clean_empty_string(business_stats_data)

print(business_stats_data[-5])


# 1st: login to database
conn = pgconnect()

# if you want to reset the table
pgexec(conn, "DROP TABLE IF EXISTS BusinessStats", None, "Reset Table BusinessStats")

# 2nd: ensure that the schema is in place

business_stats_schema = """CREATE TABLE IF NOT EXISTS BusinessStats (
                            area_id INT NOT NULL PRIMARY KEY,
                            num_businesses INT DEFAULT 0,
                            retail_trade INT DEFAULT 0,
                            accommodation_and_food_services INT DEFAULT 0,
                            health_care_and_social_assistance INT DEFAULT 0,
                            education_and_training INT DEFAULT 0,
                            arts_and_recreation_services INT DEFAULT 0
                   )"""
pgexec(conn, business_stats_schema, None, "Create Table BusinessStats")

# 3nd: load data
# IMPORTANT: make sure the header line of CSV is without spaces!

insert_stmt = """INSERT INTO BusinessStats(area_id,num_businesses,retail_trade,accommodation_and_food_services,health_care_and_social_assistance,education_and_training,arts_and_recreation_services) VALUES (%(area_id)s, %(num_businesses)s, %(retail_trade)s, %(accommodation_and_food_services)s,%(health_care_and_social_assistance)s, %(education_and_training)s, %(arts_and_recreation_services)s)"""

for row in business_stats_data:
    pgexec(conn, insert_stmt, row, "row inserted")


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


# check content of Organisations table
query_stmt = "SELECT * FROM BusinessStats"
print(query_stmt)
pgquery(conn, query_stmt, None)
conn.close()

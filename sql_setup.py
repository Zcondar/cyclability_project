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


# function that takes the data and if the value is empty, change it to NULL
def clean_empty_string(data):
    for row in data:
        for key, value in row.items():
            if value == "":
                row[key] = None


def create_table(file, queries):
    data = list(csv.DictReader(open(file + '.csv')))
    clean_empty_string(data)
    # to reset table
    pgexec(conn, "DROP TABLE IF EXISTS " + file, None, "Reset Table " + file)
    # create table using schema
    pgexec(conn, queries[0], None, "Create Table " + file)
    # insert values to table
    for row in data:
        pgexec(conn, queries[1], row, "row inserted")


if __name__ == "__main__":
    # login to database
    conn = pgconnect()

    # =====Queries for creating and inserting table=====
    business_stats_schema = """CREATE TABLE IF NOT EXISTS BusinessStats (
                                        area_id INT NOT NULL PRIMARY KEY,
                                        num_businesses INT,
                                        retail_trade INT,
                                        accommodation_and_food_services INT,
                                        health_care_and_social_assistance INT,
                                        education_and_training INT,
                                        arts_and_recreation_services INT
                               )"""
    business_stats_insert_stmt = """INSERT INTO BusinessStats(
                                            area_id,
                                            num_businesses,
                                            retail_trade,
                                            accommodation_and_food_services,
                                            health_care_and_social_assistance,
                                            education_and_training,
                                            arts_and_recreation_services)
                                            VALUES (
                                            %(area_id)s,
                                            %(num_businesses)s,
                                            %(retail_trade)s,
                                            %(accommodation_and_food_services)s,
                                            %(health_care_and_social_assistance)s,
                                            %(education_and_training)s,
                                            %(arts_and_recreation_services)s)"""

    bike_pods_schema = """CREATE TABLE IF NOT EXISTS BikeSharingPods(
                            station_id INT NOT NULL PRIMARY KEY,
                            name VARCHAR(70),
                            num_bikes INT,
                            num_scooters INT,
                            latitude FLOAT,
                            longitude FLOAT,
                            description VARCHAR(500)
                        )"""
    bike_pods_insert_stmt = """INSERT INTO BikeSharingPods(
                                station_id,
                                name,
                                num_bikes,
                                num_scooters,
                                latitude,
                                longitude,
                                description)
                                VALUES (
                                %(station_id)s,
                                %(name)s,
                                %(num_bikes)s,
                                %(num_scooters)s,
                                %(latitude)s,
                                %(longitude)s,
                                %(description)s)"""

    census_stats_schema = """CREATE TABLE IF NOT EXISTS CensusStats(
                                area_id INT NOT NULL PRIMARY KEY,
                                median_annual_household_income INT,
                                avg_monthly_rent INT
                            )"""
    census_stats_insert_stmt = """INSERT INTO CensusStats(
                                    area_id,
                                    median_annual_household_income,
                                    avg_monthly_rent)
                                    VALUES (
                                    %(area_id)s,
                                    %(median_annual_household_income)s,
                                    %(avg_monthly_rent)s)"""

    neighbourhoods_schema = """CREATE TABLE IF NOT EXISTS Neighbourhoods(
                                area_id INT NOT NULL PRIMARY KEY,
                                area_name VARCHAR(70),
                                land_area FLOAT,
                                population INT,
                                number_of_dwellings INT,
                                number_of_businesses INT
                            )"""
    neighbourhoods_insert_stmt = """INSERT INTO Neighbourhoods(
                                    area_id,
                                    area_name,
                                    land_area,
                                    population,
                                    number_of_dwellings,
                                    number_of_businesses)
                                    VALUES (
                                    %(area_id)s,
                                    %(area_name)s,
                                    %(land_area)s,
                                    %(population)s,
                                    %(number_of_dwellings)s,
                                    %(number_of_businesses)s)"""

    statistical_areas_schema = """CREATE TABLE IF NOT EXISTS StatisticalAreas(
                                    area_id INT NOT NULL PRIMARY KEY,
                                    area_name VARCHAR(70),
                                    parent_area_id INT
                                )"""
    statistical_areas_insert_stmt = """INSERT INTO StatisticalAreas(
                                        area_id,
                                        area_name,
                                        parent_area_id)
                                        VALUES (
                                        %(area_id)s,
                                        %(area_name)s,
                                        %(parent_area_id)s)"""
    # =====QUERIES END=====

    # Queries stored in key value pair, with file name as key, and queries stored as lists
    queries = {
        'BusinessStats': [business_stats_schema, business_stats_insert_stmt],
        'BikeSharingPods': [bike_pods_schema, bike_pods_insert_stmt],
        'CensusStats': [census_stats_schema, census_stats_insert_stmt],
        'Neighbourhoods': [neighbourhoods_schema, neighbourhoods_insert_stmt],
        'StatisticalAreas': [statistical_areas_schema, statistical_areas_insert_stmt]
    }

    # Loop through each file and queries to create a table
    for k, v in queries.items():
        create_table(k, v)
    
    conn.close()

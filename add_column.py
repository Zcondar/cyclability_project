from pg_tools import pgconnect
from pg_tools import pgexec
from pg_tools import pgquery

def create_column(conn, col_name, table_name, type):
    query = """ALTER TABLE {}
                DROP COLUMN IF EXISTS {}, 
                ADD COLUMN {} {};""".format(table_name, col_name, col_name, type)

    pgexec(conn, query, None, "Created Column " + col_name + " on " + table_name)

def update_column_with_another(conn, col_name, table_name, value):
    query = """UPDATE {}
               SET {} = COALESCE{}""".format(table_name, col_name, value)

    pgexec(conn, query, None, "Update " + col_name + " on " + table_name +" with " + value)

def fix_NULL(item):
    if (item == None):
        return 0
    
    return item


def update_service_balance(conn):
    subquery = """SELECT * FROM neighbourhoods JOIN businessstats USING (area_id);"""
    result = pgquery(conn, subquery, None)
    for row in result:
        recreation = row[-1]
        education = row[-2]
        health = row[-3]
        food = row[-4]
        retail = row[-5]
        recreation = fix_NULL(recreation)
        education = fix_NULL(education)
        food = fix_NULL(food)
        retail = fix_NULL(retail)
        health = fix_NULL(health)
        
        sum = recreation + education + health + food + retail
        if (sum == 0):
            continue
        

        service_balance = (education * 5 + food * 4 + retail * 3 + recreation * 2 + health) / sum

        query = """UPDATE neighbourhoods SET service_balance = {} WHERE area_id = {}""".format(service_balance, result[0])
        pgexec(conn, query, None, "Set service balance of area " + str(result[0]) + " to " + str(service_balance))
        

if __name__ == "__main__":
    conn = pgconnect()

    create_column(conn, "population_density", "neighbourhoods", "DOUBLE PRECISION")
    create_column(conn, "dwelling_density", "neighbourhoods", "DOUBLE PRECISION")
    create_column(conn, "service_balance", "neighbourhoods", "DOUBLE PRECISION")
    create_column(conn, "bikepod_density", "neighbourhoods", "DOUBLE PRECISION")
    update_column_with_another(conn, "population_density", "neighbourhoods", "(population / land_area)")
    update_column_with_another(conn, "dwelling_density", "neighbourhoods", "(number_of_dwellings / land_area)")

    update_service_balance(conn)







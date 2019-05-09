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

if __name__ == "__main__":
    conn = pgconnect()

    create_column(conn, "population_density", "neighbourhoods", "DOUBLE PRECISION")
    create_column(conn, "dwelling_density", "neighbourhoods", "DOUBLE PRECISION")
    create_column(conn, "service_balance", "neighbourhoods", "DOUBLE PRECISION")
    create_column(conn, "bikepod_density", "neighbourhoods", "DOUBLE PRECISION")
    update_column_with_another(conn, "population_density", "neighbourhoods", "(population / land_area)")
    update_column_with_another(conn, "dwelling_density", "neighbourhoods", "(number_of_dwellings / land_area)")






from pg_tools import pgconnect
from pg_tools import pgexec
from pg_tools import pgquery
from scores import create_column

def fix_NULL(item):
    if (item == None):
        return 0
    
    return item

conn = pgconnect()
create_column(conn, "cyclability_score", "neighbourhoods", "DOUBLE PRECISION")

avg_pd = pgquery(conn, "SELECT AVG(population_density) FROM neighbourhoods;", None)
avg_dd = pgquery(conn, "SELECT AVG(dwelling_density) FROM neighbourhoods;", None)
avg_sb = pgquery(conn, "SELECT AVG(service_balance) FROM neighbourhoods;", None)
avg_bd = pgquery(conn, "SELECT AVG(bikepod_density) FROM neighbourhoods;", None)

std_pd = pgquery(conn, "SELECT STDDEV(population_density) FROM neighbourhoods;", None)
std_dd = pgquery(conn, "SELECT STDDEV(dwelling_density) FROM neighbourhoods;", None)
std_sb = pgquery(conn, "SELECT STDDEV(service_balance) FROM neighbourhoods;", None)
std_bd = pgquery(conn, "SELECT STDDEV(bikepod_density) FROM neighbourhoods;", None)

avg_pd = float(avg_pd[0][0])
avg_dd = float(avg_dd[0][0])
avg_sb = float(avg_sb[0][0])
avg_bd = float(avg_bd[0][0])

std_pd = float(std_pd[0][0])
std_dd = float(std_dd[0][0])
std_sb = float(std_sb[0][0])
std_bd = float(std_bd[0][0])

print(avg_pd)
result = pgquery(conn, "SELECT * FROM neighbourhoods", None)

def additional_score():
    return 0

for row in result:
    z_score = (float(fix_NULL(row[-3])) - avg_bd) / std_bd
    z_score += (float(fix_NULL(row[-4])) - avg_sb) / std_sb
    z_score += (float(fix_NULL(row[-5])) - avg_dd) / std_dd
    z_score += (float(fix_NULL(row[-6])) - avg_pd) / std_pd
    z_score += additional_score()

    query = "UPDATE neighbourhoods SET cyclability_score = {} WHERE area_id = {}".format(z_score, row[0])
    pgexec(conn, query, None, "Updating score for {}".format(row[1]))



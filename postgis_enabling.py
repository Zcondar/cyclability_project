from pg_tools import pgquery
from pg_tools import pgconnect


cmd = """create extension postgis;

create extension fuzzystrmatch;

create extension postgis_tiger_geocoder;

create extension postgis_topology;

"""

conn = pgconnect()
pgquery(conn, cmd, "enabling postgis")


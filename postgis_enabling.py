from pg_tools import pgquery
from pg_tools import pgconnect
from pg_tools import pgexec


cmd = """
create extension IF NOT EXISTS postgis;

create extension IF NOT EXISTS fuzzystrmatch;

create extension IF NOT EXISTS postgis_tiger_geocoder;

create extension IF NOT EXISTS postgis_topology;

"""

conn = pgconnect()
pgexec(conn, cmd, None, "enabling postgis")


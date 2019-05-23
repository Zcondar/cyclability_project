from pg_tools import pgconnect
from pg_tools import pgexec
from pg_tools import pgquery
from scores import create_column

conn = pgconnect()


create_column(conn, "geom", "bikesharingpods", "geometry(POINT, 4326);")
pgexec(conn, "update bikesharingpods set geom=st_SetSrid(st_MakePoint(longitude, latitude), 4326);", None, "")
pgexec(conn, "CREATE INDEX IF NOT EXISTS bike_geo_idx ON bikesharingpods USING GIST(geom);", None, "Creating bike geo index")


update_stmt = """UPDATE neighbourhoods N2 SET bikepod_density = 
                ((SELECT COUNT(name) FROM bikesharingpods B JOIN neighbourhoods N ON (ST_CONTAINS(N.geom, B.geom)) WHERE N2.area_id = N.area_id) / N2.land_area)"""

pgexec(conn, update_stmt, None, "")


import shapefile
from pg_tools import pgquery
from pg_tools import pgconnect
from pg_tools import pgexec
import re

sf = shapefile.Reader("1270055001_sa2_2016_aust_shape/SA2_2016_AUST.shp", encoding="iso-8859-1")
# which shpe type is it?
print(sf)

print(sf.fields)

for i in range(0, 10):
    print(sf.record(i))

query = """CREATE TABLE geom_locations (
            area_id INT PRIMARY KEY,
            geom GEOMETRY(Polygon,4326)); 
            """
conn = pgconnect()
pgexec(conn, "DROP TABLE IF EXISTS geom_locations;", None, "Drop geom_locations")
pgexec(conn, query, None, "Create geom_locations")

insert_stmt = """INSERT INTO geom_locations VALUES ( %(area_id)s, ST_GEOMFROMTEXT(%(geom)s, 4326) )"""

shapes = sf.shapes()
records= sf.records()


areas = pgquery(conn, "SELECT area_id FROM neighbourhoods;", "Find existing areas")

area_ids = []
for r in areas:
    area_ids.append(r[0])


row = {}
for i in range(0, len(shapes)):
    record = sf.record(i)

    if int(record[0]) in area_ids:
        shape  = sf.shape(i)

        row['area_id']=record[0]
        
        # prepare the polygon data
        # this is a bit complex with our dataset as it has complex polygons, some with multiple parts...
        row['geom']="POLYGON(("
        i=0
        for x, y in shape.points:
            row['geom']+="%s %s," % (x,y)
            # check for start of a new polygon part
            i += 1
            if i in shape.parts:
                row['geom']= re.sub(",$", "),(", row['geom'])
        # properly end the polygon string
        row['geom'] = re.sub(",$", "))", row['geom'])
        
        # finally: insert new row into the table
        pgexec(conn, insert_stmt, args=row, msg="inserted "+str(record[2]))

index_command = "CREATE INDEX area_idx ON geom_locations USING GIST (geom);"
pgexec(conn, index_command, None, "Created index")

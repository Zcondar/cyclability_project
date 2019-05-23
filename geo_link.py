import shapefile
from pg_tools import pgquery
from pg_tools import pgconnect
from pg_tools import pgexec
import re
from scores import create_column

sf = shapefile.Reader("1270055001_sa2_2016_aust_shape/SA2_2016_AUST.shp", encoding="iso-8859-1")
# which shpe type is it?
print(sf)

print(sf.fields)

for i in range(0, 10):
    print(sf.record(i))

conn = pgconnect()
create_column(conn, "geom", "neighbourhoods", "GEOMETRY(Polygon, 4326)")

update_stmt = """UPDATE neighbourhoods SET geom = ST_GEOMFROMTEXT(%(geom)s, 4326) WHERE area_id = %(area_id)s;"""

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
        pgexec(conn, update_stmt, args=row, msg="inserted "+str(record[2]))

index_command = "CREATE INDEX area_idx ON neighbourhoods USING GIST (geom);"
pgexec(conn, index_command, None, "Created spatial index")

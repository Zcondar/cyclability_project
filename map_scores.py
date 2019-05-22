from pg_tools import pgconnect
from pg_tools import pgexec
from pg_tools import pgquery
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

conn = pgconnect()

query = "SELECT geom, cyclability_score FROM neighbourhoods;"
gpd_df = gpd.GeoDataFrame.from_postgis(query, conn, geom_col='geom')

query = "SELECT MAX(cyclability_score), MIN(cyclability_score) FROM neighbourhoods;"
result = pgquery(conn, query, None)
v_max = result[0][0]
v_min = result[0][1]
fig1, ax = plt.subplots(1, figsize=(10, 6))

plt.figure()
plot1 = gpd_df.plot(column = "cyclability_score", cmap = "Blues", linewidth = 0.8, ax = ax, edgecolor = '0.8')
plot1.axis("off")
plot1.set_title("Cyclability Score in Sydney", fontdict={"fontsize": "14", "fontweight" : "2"})

fig1 = plot1.get_figure()
sm = plt.cm.ScalarMappable(cmap="Blues", norm=plt.Normalize(vmin=v_min, vmax=v_max))
sm._A = []
fig1.colorbar(sm)
fig1.savefig("map_overlay")

conn.close()

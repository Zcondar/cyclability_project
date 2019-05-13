import pandas as pd
import requests
data=pd.read_csv("BikeSharingPods.csv")
lat=data['latitude']
lon=data['longitude']
for i in range(len(lat)):
    pos.append((lat[i],lon[i]))
for i in range(len(lat)):
    cur_lat=lat[i]
    cur_lon=lon[i]
    website='https://api.darksky.net/forecast/84a94027c16869c8bbc86bf3ef864e07/'+str(cur_lat)+','+str(cur_lon)
    re = requests.get(website)
    jsonweb =re.json()
    print(jsonweb)


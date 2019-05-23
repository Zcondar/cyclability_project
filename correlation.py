import pandas as pd
# import numpy as np
import matplotlib.pyplot as plt
import seaborn as sb; sb.set()
# import math
from pg_tools import pgconnect
# from pg_tools import pgexec
from pg_tools import pgquery
# from scores import create_column

conn = pgconnect()
result = pgquery(conn, "SELECT * FROM neighbourhoods JOIN censusstats USING (area_id) ORDER BY area_id;", None)
z_score = []
income = []
rent = []
for row in result:
    z_score.append(row[-3])
    income.append(row[-2])
    rent.append(row[-1])

score_income = pd.DataFrame({"score": z_score, "income": income})
score_rent = pd.DataFrame({"score": z_score, "rent": rent})

plt.figure()
plot1 = sb.regplot(x = "score", y= "income", data = score_income, fit_reg = False)
fig1 = plot1.get_figure()
fig1.savefig("score_income")

plt.figure()
plot2 = sb.regplot(x = "score", y = "rent", data = score_rent, fit_reg = False)
fig2 = plot2.get_figure()
fig2.savefig("score_rent")
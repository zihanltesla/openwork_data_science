import pandas as pd
import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="openwork",
        user="postgres",
        password="756100"
    )
    print("successfully connected to PostgreSQL！")
except Exception as e:
    print("❌ 连接失败:", e)

finally:
    if 'conn' in locals():
        
        conn.close()

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://postgres:756100@localhost:5432/openwork")

# df = pd.read_sql("SELECT * FROM advisers LIMIT 10;", engine)
# print(df)

query = """
SELECT f.region,
       COUNT(DISTINCT c.client_id) AS clients,
       SUM(c.aum) AS total_aum,
       ROUND(AVG(c.aum),2) AS avg_aum
FROM clients c
JOIN advisers a ON c.adviser_id = a.adviser_id
JOIN firms f ON a.firm_id = f.firm_id
GROUP BY f.region
ORDER BY total_aum DESC;
"""
df = pd.read_sql(query, engine)
print(df)

import matplotlib.pyplot as plt

df.plot(kind='bar', x='region', y='total_aum', legend=False)
plt.title("Total AUM by Region")
plt.ylabel("AUM (Total Assets Under Management)")
plt.tight_layout()
plt.show()

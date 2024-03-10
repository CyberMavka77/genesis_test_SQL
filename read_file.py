import pandas as pd
import psycopg2
import time

time.sleep(3)
df = pd.read_csv("/opt/app/subscription1.csv")

conn = psycopg2.connect(
    database="gen_test_db",
    user="docker",
    password="docker",
    host="database",
    port="5432"
)

cur = conn.cursor()
cur.execute("SELECT * FROM information_schema.tables WHERE table_name = 'automotive_data';")

if not bool(cur.rowcount):
    cur.execute(open("/opt/app/DDL.sql", "r").read())

for index, row in df.iterrows():
    line = [el.strip() if el.strip() != "?" else "NULL" for el in row.to_string(header=False, index=False).split('\n')]
    line = [el if (el.isnumeric() or el == "NULL") else f"'{el}'" for el in line]
    cur.execute(f"INSERT INTO gen_test.data VALUES ({index + 1},{','.join(line)})")

query1 = """WITH UserPurchases AS (
    SELECT
        user_id,
        COUNT(DISTINCT purchase_date) AS purchase_count
    FROM
        gen_test.data WHERE product_id = 'tenwords_1w_9.99_offer' GROUP BY user_id
)
SELECT CAST(users_2 AS REAL) / users_1 AS CR FROM
    (SELECT COUNT(user_id) AS users_2 FROM UserPurchases WHERE UserPurchases.purchase_count > 1),
    (SELECT COUNT(user_id) AS users_1 FROM UserPurchases);
"""

cur.execute(query1)
rows = cur.fetchall()
for row in rows:
    print(row)

query2 = """ALTER TABLE gen_test.data ADD transaction_number INT;
UPDATE gen_test.data SET transaction_number = subquery.transaction_number
FROM (
    SELECT
        user_id, purchase_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY purchase_date) AS transaction_number FROM gen_test.data
) AS subquery
WHERE
    gen_test.data.user_id = subquery.user_id AND gen_test.data.purchase_date = subquery.purchase_date;
SELECT * FROM gen_test.data limit 10;
"""

cur.execute(query2)
rows = cur.fetchall()
for row in rows:
    print(row)

query3 = """SELECT COUNT(DISTINCT transaction_number) FROM gen_test.data;"""
cur.execute(query3)
rows = cur.fetchall()
for row in rows:
    print(row)

query4 = """SELECT user_id FROM gen_test.data
WHERE transaction_number = 1 AND refunded = 'False' AND user_id IN (
    SELECT user_id FROM gen_test.data WHERE transaction_number = 2 AND refunded = 'True');"""

cur.execute(query4)
rows = cur.fetchall()
for row in rows:
    print(row)

cur.close()
conn.close()

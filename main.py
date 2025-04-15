from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import os

import os
print(os.path.exists('/secure-connect-db1.zip'))  # should print True

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json


cloud_config= {
  'secure_connect_bundle': '/secure-connect-db1.zip'
}


with open("/db1-token.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
  print(row[0])
else:
  print("An error occurred.")

session.set_keyspace("sales_keyspace")

session.execute("""
    CREATE TABLE IF NOT EXISTS bronze_sales (
        id UUID PRIMARY KEY,
        date text,
        product text,
        quantity int,
        price decimal,
        region text,
        category text,
        sales_person text
    );
""")

print("Bronze table created successfully!")

df = pd.read_csv('/sales_100.csv')
print(df.head(10))

import uuid

bronze_df = pd.DataFrame({
    'date': df['Order Date'],
    'product': df['Item Type'],
    'quantity': df['UnitsSold'],
    'price': df['UnitPrice'],
    'region': df['Region'],
    'category': df['Order Priority'],
    'sales_person': df['Country']  # You can change this to another field if needed
})

for index, row in bronze_df.iterrows():
    session.execute("""
        INSERT INTO bronze_sales (id, date, product, quantity, price, region, category, sales_person)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        uuid.uuid4(),
        row['date'],
        row['product'],
        int(row['quantity']),
        float(row['price']),
        row['region'],
        row['category'],
        row['sales_person']
    ))

session.execute("""
    CREATE TABLE IF NOT EXISTS silver_sales (
        id UUID PRIMARY KEY,
        order_date date,
        product text,
        quantity int,
        price decimal,
        revenue decimal,
        region text,
        category text,
        sales_person text
    );
""")

from datetime import datetime
import uuid

rows = session.execute("SELECT * FROM bronze_sales")

for row in rows:
    try:

        order_date = datetime.strptime(row.date, '%m/%d/%Y').date()

        quantity = int(row.quantity)
        price = float(row.price)
        revenue = quantity * price

        session.execute("""
            INSERT INTO silver_sales (
                id, order_date, product, quantity, price,
                revenue, region, category, sales_person
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            uuid.uuid4(),
            order_date,
            row.product,
            quantity,
            price,
            revenue,
            row.region,
            row.category,
            row.sales_person
        ))
    except Exception as e:
        print(f"Skipping row due to error: {e}")

session.execute("""
    CREATE TABLE IF NOT EXISTS gold_revenue_by_region (
        region text,
        total_revenue decimal,
        PRIMARY KEY (region)
    );
""")

from collections import defaultdict

region_revenue = defaultdict(float)

rows = session.execute("SELECT region, revenue FROM silver_sales")
for row in rows:
    region_revenue[row.region] += float(row.revenue)

for region, total in region_revenue.items():
    session.execute("""
        INSERT INTO gold_revenue_by_region (region, total_revenue)
        VALUES (%s, %s)
    """, (region, total))

session.execute("""
    CREATE TABLE IF NOT EXISTS gold_product_sales (
        product text,
        total_sold int,
        PRIMARY KEY (product)
    );
""")

product_sales = defaultdict(int)

rows = session.execute("SELECT product, quantity FROM silver_sales")
for row in rows:
    product_sales[row.product] += int(row.quantity)

for product, total in product_sales.items():
    session.execute("""
        INSERT INTO gold_product_sales (product, total_sold)
        VALUES (%s, %s)
    """, (product, total))

session.execute("""
    CREATE TABLE IF NOT EXISTS gold_sales_by_person (
        sales_person text,
        total_revenue decimal,
        PRIMARY KEY (sales_person)
    );
""")

person_sales = defaultdict(float)

rows = session.execute("SELECT sales_person, revenue FROM silver_sales")
for row in rows:
    person_sales[row.sales_person] += float(row.revenue)

for person, total in person_sales.items():
    session.execute("""
        INSERT INTO gold_sales_by_person (sales_person, total_revenue)
        VALUES (%s, %s)
    """, (person, total))

rows = session.execute("SELECT * FROM gold_sales_by_person")
for r in rows:
    print(r)

rows = session.execute("SELECT * FROM gold_product_sales")
for r in rows:
    print(r)

rows = session.execute("SELECT * FROM gold_revenue_by_region")
for r in rows:
    print(r)


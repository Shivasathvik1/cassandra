sales Data Processing with Cassandra in Google Colab
This project walks through building a data pipeline using Google Colab and Apache Cassandra to clean, organize, and analyze sales data. It takes raw CSV input, transforms it, and creates summaries at different levels using a layered architecture: Bronze, Silver, and Gold.

What This Project Does
Connects to a Cassandra database using Astra DB

Uploads and reads sales data from a CSV file

Stores raw data (Bronze layer)

Cleans and enriches data (Silver layer)

Aggregates key business metrics (Gold layer)

Tools & Libraries
Python (Pandas, UUID, JSON, datetime)

Cassandra (cassandra-driver)

Google Colab (no installation needed!)

Astra DB (secure cloud Cassandra)

Key Steps
Setup Connection

Upload secure-connect-db1.zip and db1-token.json

Authenticate with client ID and secret

Bronze Table

Raw sales records from sales_100.csv go here

Basic fields: date, product, quantity, price, etc.

Silver Table

Cleaned data with real dates and revenue calculation

Revenue = quantity × price

Gold Tables

gold_revenue_by_region: Total revenue by region

gold_product_sales: Quantity sold by product

gold_sales_by_person: Revenue by salesperson

Results

Outputs key business insights to help understand performance trends

File Requirements
sales_100.csv — your raw sales data

secure-connect-db1.zip — connection bundle from Astra

db1-token.json — your authentication token
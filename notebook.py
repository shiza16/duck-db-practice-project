import pandas as pd
import glob
import datetime, time
import duckdb

conn = duckdb.connect() # create an in-memory database

# with pandas
cur_time = time.time()
df = pd.concat([pd.read_csv(f) for f in glob.glob('dataset/*.csv')])
print(f"time: {(time.time() - cur_time)}")
print(df.head(10))

# with duckdb
cur_time = time.time()
df = conn.execute("""
	SELECT *
	FROM 'dataset/*.csv'
	LIMIT 10
""").df()
print(f"time: {(time.time() - cur_time)}")
print(df)

#check the types of dataframe
conn.register("df_view", df)
conn.execute("DESCRIBE df_view").df() # doesn't work if you don't register df as a virtual table

conn.execute("SELECT COUNT(*) FROM df_view").df()

# droping nulls
df.isnull().sum()
df = df.dropna(how='all')

# Here we use df and not df_view
# With DuckDB we can run SQL queries on top of Pandas dataframes
conn.execute("SELECT COUNT(*) FROM df").df()

#create a table and load the data 
""" A View/Virtual Table is a SELECT statement. 
That statement is run every time the view is referenced in a query.
Views are great for abstracting the complexity of the underlying tables they reference."""

conn.execute("""
CREATE OR REPLACE TABLE sales AS
	SELECT
		"Order ID"::INTEGER AS order_id,
		Product AS product,
		"Quantity Ordered"::INTEGER AS quantity,
		CAST(REPLACE(Price, ',', '') AS DECIMAL) AS price_each,
		strptime(Order_Date::STRING, '%Y-%m-%d %H:%M:%S')::timestamp as order_date,
		"Purchase Address" AS purchase_address
	FROM df
	WHERE
		TRY_CAST("Order ID" AS INTEGER) NOTNULL
""")
# TRY_CAST returns null if the cast fails. By using TRY_CAST NOTNULL we skip malformatted rows.

conn.execute("FROM sales").df()

## Exclude clause to exclude some columns
conn.execute("""
	SELECT 
		* EXCLUDE (product, order_date, purchase_address)
	FROM sales
	""").df()

# using min 
conn.execute("""
	SELECT 
		MIN(COLUMNS(* EXCLUDE (product, order_date, purchase_address))) 
	FROM sales
	""").df()

# it's the alternative of the below command
# Execute the query
result_df = conn.execute("""
    SELECT 
        MIN(order_id) AS min_order_id,
        MIN(quantity) AS min_quantity,
        MIN(price_each) AS min_price_each
    FROM sales
""").df()

print(result_df)

# Since VIEWS are recreated each time a query reference them, if new data is added to the sales table, the VIEW gets updated as well
conn.execute("""
	CREATE OR REPLACE VIEW aggregated_sales AS
	SELECT
		order_id,
		COUNT(1) as nb_orders,
		MONTH(order_date) as month,
		str_split(purchase_address, ',')[2] AS city,
		SUM(quantity * price_each) AS revenue
	FROM sales
	GROUP BY ALL
""")

# Export to parquet files 

conn.execute("COPY (FROM aggregated_sales) TO 'aggregated_sales.parquet' (FORMAT 'parquet')")

# query from parquet files
print(conn.execute("FROM aggregated_sales.parquet").df())

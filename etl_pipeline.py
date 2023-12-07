import psycopg2
import sys
import os 
import pandas as pd
from config import table_names, columns_name, parse_dates_columns
## User and Password for postgre server
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']
# uid = 'postgres'
# pwd = '02072002xxx'
def connect_to_postgresql():
    try:
        conn = psycopg2.connect(
        database="sales", user=uid, password=pwd, host='localhost', port='5432'
        )
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful!")
    return conn
def sql_to_dataframe(conn, query, column_names, parse_dates=[]):
    cursor = conn.cursor()
    try: 
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        conn.close()
        return 1
    tuples_list = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(tuples_list, columns=column_names)
    for i in parse_dates:
        df[i] = pd.to_datetime(df[i], format='%Y-%m-%d')
    return df
    
def main():
    # Load data from Postgresql database
    conn = connect_to_postgresql()
    conn.autocommit = True
    df = {}
    for i in table_names:
        df[i] = sql_to_dataframe(conn, """SELECT * FROM {}""".format(i), columns_name[i],parse_dates_columns[i])
        print("Success load {}".format(i))
    
    conn.close()
    
    # Tranform
    # Create dimension inventory table
    
    dim_inventory = pd.concat([df["begin_inventory"], df["end_inventory"]]).drop_duplicates()
    dim_inventory = dim_inventory.drop_duplicates(subset=["inventory_id"])
    dim_inventory = dim_inventory.drop(["description","size","onHand","price","start_date"], axis=1)
    # print(dim_inventory.info())
    
    # Create dimension product table
    
    dim_product = df["purchase_price_description"].drop_duplicates(subset=["brand"])
    dim_product = dim_product.drop(["classification"], axis=1)
    # print(dim_product.info())
    
    # Create dimension sales_date
    
    dim_sales_date = df["final_sales"].drop_duplicates(subset=["sales_date"]).copy()
    dim_sales_date['year'] = dim_sales_date['sales_date'].dt.year
    dim_sales_date['month'] = dim_sales_date['sales_date'].dt.month
    dim_sales_date['day'] = dim_sales_date['sales_date'].dt.day
    dim_sales_date = dim_sales_date.drop(["inventory_id", "store", "brand", "description", "size", "sales_quantity", "sales_dollar", "sales_price", "volume", "classification", "excise_tax", "vendor_no", "vendor_name"], axis=1)
    # print(dim_sales_date.info())
    
    # Create sales fact table
    
    sales_fact_table = df["final_sales"].drop(["store", "description", "size", "volume", "classification","vendor_no", "vendor_name"], axis=1).copy()
    # print(sales_fact_table.info())
    
    # Load to SQL Server 
    
main()

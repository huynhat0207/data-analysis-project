import psycopg2
import pyodbc
import sys
import os 
import pandas as pd
from config import table_names, columns_name, parse_dates_columns
## User and Password for postgre server
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

server = '' 
database = '' 
username = '' 
password = ''

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
    temp = df["final_sales"].drop_duplicates(subset=["inventory_id"]).copy()
    temp = temp.drop(["sales_date", "description", "size", "sales_quantity", "sales_dollar", "sales_price", "volume", "classification", "excise_tax", "vendor_no", "vendor_name"], axis=1)
    temp["city"] = temp["inventory_id"].str.split('_').str[1]
    dim_inventory = pd.concat([temp, dim_inventory]).drop_duplicates(subset=["inventory_id"])
    
    
    # Create dimension product table
    
    dim_product = df["purchase_price_description"].drop_duplicates(subset=["brand"])
    dim_product = dim_product.drop(["classification"], axis=1)
    dim_product.dropna(axis=0)
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
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    
    print("Load to Dim_Inventory")
    for index, row in dim_inventory.iterrows():
        try:
            # print("""INSERT INTO Dim_Inventory (Inventory_Id,Brand,Store,City) values('{}',{},{},'{}')""".format(str(row.inventory_id).replace("\'", "\'\'"), int(row.brand),  int(row.store), str(row.city).replace("\'", "\'\'")))
            cursor.execute("""INSERT INTO Dim_Inventory (Inventory_Id,Brand,Store,City) values('{}',{},{},'{}')""".format(str(row.inventory_id).replace("\'", "\'\'"), int(row.brand),  int(row.store), str(row.city).replace("\'", "\'\'")))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
            user_input = input("Type something and press Enter to continue: ")
    print("Load to Dim_Product")
    for index, row in dim_product.iterrows():
        try:
            # print("INSERT INTO Dim_Product (Brand, Description, Price, Size, Volume, Purchase_price, Vendor_num, Vendor_name) values({},'{}',{},'{}',{},{},{},'{}')".format(int(row.brand), str(row.description).replace("\'", "\'\'"), float(row.price), str(row.size).replace("\'", "\'\'"), float(row.volume), float(row.purchase_price), row.vendor_num, str(row.vendor_name).replace("\'", "\'\'")))
            cursor.execute("INSERT INTO Dim_Product (Brand, Description, Price, Size, Volume, Purchase_price, Vendor_num, Vendor_name) values({},'{}',{},'{}',{},{},{},'{}')".format(int(row.brand), str(row.description).replace("\'", "\'\'"), float(row.price), str(row.size).replace("\'", "\'\'"), float(row.volume), float(row.purchase_price), row.vendor_num, str(row.vendor_name).replace("\'", "\'\'")))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
            user_input = input("Type something and press Enter to continue: ")
    print("Load to Dim_Sales_Date")
    for index, row in dim_sales_date.iterrows():
        try:
            # print("INSERT INTO Dim_Sales_Date (Sales_date, Year, Month, Day) values('{}',{},{},{})".format(row.sales_date.date(), int(row.year), int(row.month), int(row.day)))
            cursor.execute("INSERT INTO Dim_Sales_Date (Sales_date, Year, Month, Day) values('{}',{},{},{})".format(row.sales_date.date(), int(row.year), int(row.month), int(row.day)))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
            user_input = input("Type something and press Enter to continue: ")
    print("Load to Sales_Fact_Table")
    for index, row in sales_fact_table.iterrows():
        try:
            # print("INSERT INTO Sales_Fact_Table (Inventory_Id, Brand, Sales_date, Sales_dollar, Sales_price, Sales_quantity, Excise_tax) values('{}',{},'{}',{},{},{},{})".format(row.inventory_id.replace("\'", "\'\'"), row.brand, row.sales_date.date(), row.sales_dollar, row.sales_price, row.sales_quantity, row.excise_tax))
            cursor.execute("INSERT INTO Sales_Fact_Table (Inventory_Id, Brand, Sales_date, Sales_dollar, Sales_price, Sales_quantity, Excise_tax) values('{}',{},'{}',{},{},{},{})".format(row.inventory_id.replace("\'", "\'\'"), row.brand, row.sales_date.date(), row.sales_dollar, row.sales_price, row.sales_quantity, row.excise_tax))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
    
    # finally:
    #     cnxn.close()
    cnxn.close()
main()

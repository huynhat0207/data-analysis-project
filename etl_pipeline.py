import psycopg2
import pyodbc
import sys
import os 
import pandas as pd
from config import table_names, columns_name, parse_dates_columns
## User and Password for postgre server
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

server = os.environ['SQLSERVER'] 
database = os.environ['SQLDATABASE']  
username = os.environ['SQLUID'] 
password = os.environ['SQLPWD'] 

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
    # Get value to January
    df['final_sales'] = df['final_sales'][df['final_sales']['sales_date'] < '02/01/2016']
    
    # Create dimension vendor table
    dim_vendor = pd.concat([
        df["invoice_purchases"][["vendor_no","vendor_name"]], 
        df["final_sales"][["vendor_no","vendor_name"]], 
        df["purchases_final"][["vendor_no","vendor_name"]],
        df["purchase_price_description"][["vendor_no","vendor_name"]]
        ]).drop_duplicates(subset=["vendor_no"])
    # print(dim_vendor.info)
    # Create dimension inventory table
    dim_inventory = pd.concat([
        df["begin_inventory"][["inventory_id"]], 
        df["end_inventory"][["inventory_id"]], 
        df["final_sales"][["inventory_id"]],
        df["purchases_final"][["inventory_id"]]
        ]).drop_duplicates(subset=["inventory_id"])
    # dim_inventory[['store','city', 'brand']] = dim_inventory['inventory_id'].str.extract(r'(\d+)_(\w+)_(\d+)')
    dim_inventory['store'] = dim_inventory["inventory_id"].str.split('_').str[0]
    dim_inventory['city'] = dim_inventory["inventory_id"].str.split('_').str[1]
    dim_inventory['brand'] = dim_inventory["inventory_id"].str.split('_').str[2]
    dim_inventory = dim_inventory.dropna()
    
    
    # Create dimension product table
    
    dim_product = df["purchase_price_description"].drop_duplicates(subset=["brand"])
    dim_product = dim_product.drop(["classification","vendor_name"], axis=1)
    dim_product= dim_product.dropna()
    # print(dim_product.info())
    
    # Create dimension sales_date
    
    dim_sales_date = df["final_sales"].drop_duplicates(subset=["sales_date"]).copy()
    dim_sales_date['year'] = dim_sales_date['sales_date'].dt.year
    dim_sales_date['month'] = dim_sales_date['sales_date'].dt.month
    dim_sales_date['day'] = dim_sales_date['sales_date'].dt.day
    dim_sales_date = dim_sales_date.drop(["inventory_id", "store", "brand", "description", "size", "sales_quantity", "sales_dollar", "sales_price", "volume", "classification", "excise_tax", "vendor_no", "vendor_name"], axis=1)
    # print(dim_sales_date.info())
    
    # Create dimension purchase orders
    dim_purchase_orders = df["invoice_purchases"].drop(["vendor_no", "vendor_name", "quantity", "dollars", "approval"], axis=1).drop_duplicates(subset=["ponumber"]).copy()
    # print(dim_purchase_orders.info())
    # Create sales fact table
    
    sales_fact_table = df["final_sales"].drop(["brand","store", "description", "size", "volume", "classification","vendor_no", "vendor_name"], axis=1).copy()
    # print(sales_fact_table.info())
    
    # Create purchase fact table
    
    purchases_fact_table = df["purchases_final"][["ponumber", "inventory_id", "quantity", "dollar", "receiving_date"]].copy()

    # print(purchases_fact_table.info())
    
    
    # Load to SQL Server 
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    
    
    print("Load to Dim_Vendor")
    for index, row in dim_vendor.iterrows():
        try:
            # print("""INSERT INTO Dim_Vendor (Vendor_no,Vendor_name) values({},'{}')""".format(row.vendor_no, str(row.vendor_name).replace("\'", "\'\'")))
            cursor.execute("""INSERT INTO Dim_Vendor (Vendor_no,Vendor_name) values({},'{}')""".format(row.vendor_no, str(row.vendor_name).replace("\'", "\'\'")))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
            

    print("Load to Dim_Product")
    for index, row in dim_product.iterrows():
        try:
            # print("INSERT INTO Dim_Product (Brand, Description, Price, Size, Volume, Purchase_price, Vendor_no) values({},'{}',{},'{}',{},{},{})".format(int(row.brand), str(row.description).replace("\'", "\'\'"), float(row.price), str(row.size).replace("\'", "\'\'"), float(row.volume), float(row.purchase_price), row.vendor_no))
            cursor.execute("INSERT INTO Dim_Product (Brand, Description, Price, Size, Volume, Purchase_price, Vendor_no) values({},'{}',{},'{}',{},{},{})".format(int(row.brand), str(row.description).replace("\'", "\'\'"), float(row.price), str(row.size).replace("\'", "\'\'"), float(row.volume), float(row.purchase_price), row.vendor_no))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
    print("Load to Dim_Inventory")
    for index, row in dim_inventory.iterrows():
        try:
            
            # print(row.brand," ",row.store, " ",row.city, "\n")
            # print("""INSERT INTO Dim_Inventory (Inventory_Id,Brand,Store,City) values('{}',{},{},'{}')""".format(str(row.inventory_id).replace("\'", "\'\'"), int(row.brand),  int(row.store), str(row.city).replace("\'", "\'\'")))
            cursor.execute("""INSERT INTO Dim_Inventory (Inventory_Id,Brand,Store,City) values('{}',{},{},'{}')""".format(str(row.inventory_id).replace("\'", "\'\'"), int(row.brand),  int(row.store), str(row.city).replace("\'", "\'\'")))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
    print("Load to Dim_Sales_Date")
    for index, row in dim_sales_date.iterrows():
        try:
            # print("INSERT INTO Dim_Sales_Date (Sales_date, Year, Month, Day) values('{}',{},{},{})".format(row.sales_date.date(), int(row.year), int(row.month), int(row.day)))
            cursor.execute("INSERT INTO Dim_Sales_Date (Sales_date, Year, Month, Day) values('{}',{},{},{})".format(row.sales_date.date(), int(row.year), int(row.month), int(row.day)))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
    print("Load to Sales_Fact_Table")
    for index, row in sales_fact_table.iterrows():
        try:
            # print("INSERT INTO Sales_Fact_Table (Inventory_Id, Brand, Sales_date, Sales_dollar, Sales_price, Sales_quantity, Excise_tax) values('{}',{},'{}',{},{},{},{})".format(row.inventory_id.replace("\'", "\'\'"), row.brand, row.sales_date.date(), row.sales_dollar, row.sales_price, row.sales_quantity, row.excise_tax))
            cursor.execute("INSERT INTO Sales_Fact_Table (Inventory_Id, Sales_date, Sales_dollar, Sales_price, Sales_quantity, Excise_tax) values('{}','{}',{},{},{},{})".format(row.inventory_id.replace("\'", "\'\'"), row.sales_date.date(), row.sales_dollar, row.sales_price, row.sales_quantity, row.excise_tax))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
    print("Load to Dim_Purchase_Orders")
    for index, row in dim_purchase_orders.iterrows():
        try:
            # print("INSERT INTO Dim_Purchase_Orders (Inventory_Id, Brand, Sales_date, Sales_dollar, Sales_price, Sales_quantity, Excise_tax) values('{}',{},'{}',{},{},{},{})".format(row.inventory_id.replace("\'", "\'\'"), row.brand, row.sales_date.date(), row.sales_dollar, row.sales_price, row.sales_quantity, row.excise_tax))
            cursor.execute("INSERT INTO Dim_Purchase_Orders (PONumber, PODate, Pay_Date, Freight, Invoice_Date) values({},'{}','{}',{},'{}')".format(row.ponumber, row.podate.date(), row.pay_date.date(), row.feight, row.invoice_date.date()))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
    cnxn.commit()
    print("Load to Purchases_Fact_Table")
    for index, row in purchases_fact_table.iterrows():
        try:
            # print("INSERT INTO Purchases_Fact_Table (PONumber, Inventory_Id, Vendor_no, Purchase_quantity, Purchase_dollar, Receiving_date) values({},'{}',{},{},{},'{}')".format(row.ponumber, row.inventory_id.replace("\'", "\'\'"), row.vendor_no, row.quantity, row.dollar, row.invoice_date.date()))
            cursor.execute("INSERT INTO Purchases_Fact_Table (PONumber, Inventory_Id, Purchase_quantity, Purchase_dollar, Receiving_date) values({},'{}',{},{},'{}')".format(row.ponumber, row.inventory_id.replace("\'", "\'\'"), row.quantity, row.dollar, row.receiving_date.date()))
        except pyodbc.Error as ex:
            print(f"Error: {ex}")
            

    cnxn.commit()

    cursor.close()
    cnxn.close()
main()

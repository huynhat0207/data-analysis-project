USE project;
DROP TABLE IF EXISTS Sales_Fact_Table;
DROP TABLE IF EXISTS Purchases_Fact_Table;
DROP TABLE IF EXISTS Dim_Inventory;
DROP TABLE IF EXISTS Dim_Product;
DROP TABLE IF EXISTS Dim_Sales_Date;
DROP TABLE IF EXISTS Dim_Vendor;
DROP TABLE IF EXISTS Dim_Purchase_Orders;

CREATE TABLE Dim_Vendor (Vendor_no int, Vendor_name CHAR(50), PRIMARY KEY (Vendor_no))
CREATE TABLE Dim_Product ( Brand int, Description CHAR(100), Price float, Size CHAR(20), Volume float, Purchase_price float, Vendor_no int, PRIMARY KEY (Brand), FOREIGN KEY (vendor_no) REFERENCES Dim_Vendor(vendor_no) );

CREATE TABLE Dim_Inventory ( Inventory_Id CHAR(50), Store int, Brand int, City CHAR(50), PRIMARY KEY (Inventory_Id), FOREIGN KEY (Brand) REFERENCES Dim_Product(Brand) );

CREATE TABLE Dim_Sales_Date ( Sales_date date, Year int, Month int, Day int, PRIMARY KEY (Sales_date) );

CREATE TABLE Sales_Fact_Table ( Inventory_Id CHAR(50), Sales_date date, Sales_dollar float, Sales_price float, Sales_quantity int, Excise_tax float, FOREIGN KEY (Inventory_Id) REFERENCES Dim_Inventory(Inventory_Id), FOREIGN KEY (Sales_date) REFERENCES Dim_Sales_Date(Sales_date));

CREATE TABLE Dim_Purchase_Orders ( PONumber int, PODate date, Pay_Date date, Freight float, Invoice_Date date, PRIMARY KEY (PONumber));

CREATE TABLE Purchases_Fact_Table ( PONumber int, Inventory_Id CHAR(50), Purchase_quantity int, Purchase_dollar int, Receiving_date date, FOREIGN KEY (PONumber) REFERENCES Dim_Purchase_Orders(PONumber), FOREIGN KEY (Inventory_Id) REFERENCES Dim_Inventory(Inventory_Id));



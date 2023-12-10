drop table if exists final_sales;
create table final_sales(
	inventory_id varchar(50),
	store int,
	brand varchar(10),
	description varchar(50),
	size varchar(20),
	sales_quantity int,
	sales_dollar float,
	sales_price float,
	sales_date date,
	volume int,
	classification int,
	excise_tax float,
	vendor_no int,
	vendor_name varchar(50)
);
copy final_sales(inventory_id, store, brand, description, size, sales_quantity, 
				 sales_dollar, sales_price, sales_date, volume, classification, 
				excise_tax, vendor_no, vendor_name)
from '.\data\SalesFINAL12312016.csv'
DELIMITER ','
CSV HEADER;


-------------------------------------------------------------------------------------------------------------------------
drop table if exists invoice_purchases;
create table invoice_purchases(
	vendor_no int, 
	vendor_name varchar(50), 
	invoice_date date, 
	POnumber int, PODate date, 
	pay_date date, 
	quantity int, 
	Dollars float, 
	feight float, 
	approval varchar
);

-- vendor_no, vendor_name, invoice_date, POnumber, PODate, pay_date, quantity, Dollars, feight, approval
copy invoice_purchases(vendor_no, vendor_name, invoice_date, POnumber, PODate, pay_date, quantity, Dollars, feight, approval
)
from '.\data\sales\InvoicePurchases12312016.csv'
DELIMITER ','
CSV HEADER;

-------------------------------------------------------------------------------------------------------------------------
drop table if exists purchases_final;
create table purchases_final(
	inventory_id varchar(50), 
	store int, 
	brand int, 
	description varchar(50), 
	size varchar(50), 
	vendor_no int, 
	vendor_name varchar(50), 
	POnumber int, POdate date, 
	receiving_date date, 
	invoice_date date, 
	pay_date date, 
	purchase_price float, 
	quantity int, 
	dollar float, 
	classification int
);
copy purchases_final(inventory_id, store, brand, description, size, vendor_no, vendor_name, POnumber, POdate, receiving_date, invoice_date, pay_date, purchase_price, quantity, dollar, classification)
from '.\data\PurchasesFINAL12312016.csv'
DELIMITER ','
CSV HEADER;

-------------------------------------------------------------------------------------------------------------------------
drop table if exists begin_inventory;
create table begin_inventory (
	inventory_id varchar(50),
	store int,
	city varchar(50),
	brand int,
	description varchar(50),
	size varchar(20),
	onHand int,
	price float,
	start_date date
);

copy begin_inventory(inventory_id, store, city, brand, description, size, onHand, price, start_date)
from '.\data\BegInvFINAL12312016.csv'
DELIMITER ','
CSV HEADER;

-------------------------------------------------------------------------------------------------------------------------
drop table if exists end_inventory;
create table end_inventory (
	inventory_id varchar(50),
	store int,
	city varchar(50),
	brand int,
	description varchar(50),
	size varchar(20),
	onHand int,
	price float,
	start_date date
);

copy end_inventory(inventory_id, store, city, brand, description, size, onHand, price, start_date)
from '.\data\EndInvFINAL12312016.csv'
DELIMITER ','
CSV HEADER;

-------------------------------------------------------------------------------------------------------------------------
drop table if exists purchase_price_description;
create table purchase_price_description (
	brand int,
	description varchar(100),
	price float,
	size varchar(20),
	volume float,
	classification int,
	purchase_price float,
	vendor_num int,
	vendor_name varchar(50)
);

copy purchase_price_description(brand, description, price, size, volume, classification, purchase_price, vendor_num, vendor_name)
from '.\data\2017PurchasePricesDec.csv'
DELIMITER ','
CSV HEADER;
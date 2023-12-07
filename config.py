

table_names =["final_sales", "begin_inventory", "end_inventory", "invoice_purchases", "purchase_price_description", "purchases_final"]

columns_name = {
    "final_sales": ["inventory_id", "store", "brand", "description", "size", "sales_quantity", "sales_dollar", "sales_price", "sales_date", "volume", "classification", "excise_tax", "vendor_no", "vendor_name"],
    "begin_inventory": ["inventory_id", "store", "city", "brand", "description", "size", "onHand", "price", "start_date"],
    "end_inventory": ["inventory_id", "store", "city", "brand", "description", "size", "onHand", "price", "start_date"],
    "invoice_purchases": ["vendor_no", "vendor_name", "invoice_date", "ponumber","podate", "pay_date", "quantity", "dollars", "feight", "approval"],
    "purchase_price_description": ["brand", "description", "price", "size", "volume", "classification", "purchase_price", "vendor_num", "vendor_name"],
    "purchases_final": ["inventory_id", "store", "brand", "description", "size", "vendor_no", "vendor_name", "ponumber", "podate","receiving_date", "invoice_date", "pay_date", "purchase_price", "quantity", "dollar", "classification"]    
}
parse_dates_columns = {
    "final_sales": ["sales_date"],
    "begin_inventory": ["start_date"],
    "end_inventory": ["start_date"],
    "invoice_purchases": ["invoice_date","podate", "pay_date"],
    "purchase_price_description": [],
    "purchases_final": ["podate","receiving_date", "invoice_date", "pay_date"]
}
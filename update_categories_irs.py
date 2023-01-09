import frappe
import csv

file = open('updated_category.csv')
csvreader = csv.reader(file)
header = []
header = next(csvreader)
category = {}

for row in csvreader:
    category[row[0]] = row[1]

item_categories = category.copy()

irs = frappe.db.sql(f"""
    SELECT name, product_code
    FROM `tabItem Request`
    WHERE product_code IN {tuple(item_categories.keys())}
    AND picking_status = 'Picking Requested'
    AND picklist_name IS NULL
""")

print(f"Found {len(irs)} item requests")

# update IR using SQL
for ir in irs:
    print(ir)
    frappe.db.sql(f"""
        UPDATE `tabItem Request`
        SET tr_category = '{item_categories[ir[1]]}'
        WHERE name = '{ir[0]}'
    """)

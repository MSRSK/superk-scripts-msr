import frappe
import json

def main():
    def getSalesInvoices():
        # return ["SI/22-23/00196"]
        si_list = frappe.db.sql("""
        SELECT `tabSales Invoice`.`name` AS `name`
        FROM `tabSales Invoice`
        LEFT JOIN `tabDelivery Note` `TabDelivery Note` ON `tabSales Invoice`.`delivery_challan` = `TabDelivery Note`.`name`
        WHERE (`TabDelivery Note`.`creation` >= convert_tz('2023-03-20 00:00:00.000', 'Asia/Kolkata', @@session.time_zone)
        AND `TabDelivery Note`.`creation` < convert_tz('2023-03-22 00:00:00.000', 'Asia/Kolkata', @@session.time_zone))
        LIMIT 1048575
        """, as_list=True)
        dsd_si_list = frappe.db.sql("""
        SELECT `tabSales Invoice`.`name` AS `name`
        FROM `tabSales Invoice`
        WHERE (`tabSales Invoice`.`dsd_reference` IS NOT NULL
        AND (`tabSales Invoice`.`dsd_reference` <> ''
            OR `tabSales Invoice`.`dsd_reference` IS NULL) AND `tabSales Invoice`.`creation` >= convert_tz('2023-03-20 00:00:00.000', 'Asia/Kolkata', @@session.time_zone) AND `tabSales Invoice`.`creation` < convert_tz('2023-03-22 00:00:00.000', 'Asia/Kolkata', @@session.time_zone))
        LIMIT 1048575
        """)
        si_list = list(si_list) + list(dsd_si_list)
        si_flaten  = [si[0] for si in si_list]
        return si_flaten

    def getWrongPriceSalesInvoiceItems(si):
        from superk.superk.doctype.delivery_challan_creation_tool.delivery_challan_creation_tool import get_price_list_rate
        wrong_items = []
        for item in si.items:
            retail_item_price = get_price_list_rate(item.item_code, "Retail Sale Price", si.customer)
            franchisee_item_price = get_price_list_rate(item.item_code, "Franchisee Purchase Price", si.customer)
            if retail_item_price != item.retail_rate or franchisee_item_price != item.rate:
                wrong_items.append(item)
        return wrong_items

    def correctSalesInvoices():
        from superk.api.create_sales_invoice import create_sales_invoice
        import datetime
        T = datetime.datetime.now()
        T1 = datetime.datetime.now()
        si_list = getSalesInvoices()
        wrong_sis = []
        total_wrong_items = 0
        agg_cn_grand_total = 0
        agg_si_grand_total = 0
        for i, si_name in enumerate(si_list):
            T1 = datetime.datetime.now()
            print(f"[{si_name}] {i+1}/{len(si_list)} | Started")
            si = frappe.get_doc("Sales Invoice", si_name)
            wrong_items = getWrongPriceSalesInvoiceItems(si)
            if len(wrong_items):
                print(f"[{si_name}] {i+1}/{len(si_list)} | Wrong items {len(wrong_items)}")
                total_wrong_items += len(wrong_items)
                wrong_sis.append(si.name)
                create_cn_request_obj = {
                    "customer": si.customer,
                    "is_return": 1,
                    "print_note_reference": f"Wrong Price Adjust CN of [{si.name}]",
                    "source_warehouse": "Kadapa CDC - SK",
                    # "do_not_submit": True,
                    "items": [
                        {
                            "item_code": item.item_code,
                            "qty": -1*item.qty,
                            "rate": item.rate,
                            "retail_rate": item.retail_rate,
                        } for item in wrong_items
                    ]
                }
                print(f"[{si_name}] {i+1}/{len(si_list)} | CN creation started")
                cn_res = create_sales_invoice(json.dumps(create_cn_request_obj))
                cn_name = cn_res.split(" | ")[0]
                cn_grand_total = float(cn_res.split(" | ")[2].split(" ")[-1])
                agg_cn_grand_total += cn_grand_total
                print(f"[{si_name}] {i+1}/{len(si_list)} | CN creation completed - [{cn_name}]: Grand Total = {cn_grand_total}")
                create_si_request_obj = {
                    "customer": si.customer,
                    "is_return": 0,
                    "source_warehouse": "Kadapa CDC - SK",
                    # "do_not_submit": True,
                    "print_note_reference": f"Wrong Price Adjust SI of [{si.name}]",
                    "items": [
                        {
                            "item_code": item.item_code,
                            "qty": item.qty
                        } for item in wrong_items
                    ]
                }
                print(f"[{si_name}] {i+1}/{len(si_list)} | Correct SI creation started")
                si_res = create_sales_invoice(json.dumps(create_si_request_obj))
                si_name = si_res.split(" | ")[0]
                si_grand_total = float(si_res.split(" | ")[2].split(" ")[-1])
                agg_si_grand_total += si_grand_total
                print(f"[{si_name}] {i+1}/{len(si_list)} | Correct SI creation completed - [{si_name}]: Grand Total = {si_grand_total}")
                frappe.db.commit()

            print(f"[{si_name}] {i+1}/{len(si_list)} | Completed in {datetime.datetime.now() - T1} Total time till now {datetime.datetime.now() - T}")
        
        print(f"Total time taken {datetime.datetime.now() - T}")
        print(f"Total wrong Sales Invoices = {len(wrong_sis)}")
        print(f"Total wrong items = {total_wrong_items}")
        print(f"Total CN Grand Total = {agg_cn_grand_total}")
        print(f"Total SI Grand Total = {agg_si_grand_total}")
        print(f"Total Excess Billed = {-1 * agg_cn_grand_total - agg_si_grand_total}")
    correctSalesInvoices()

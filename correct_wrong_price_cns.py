import frappe
import json

def main():
    def getCreditNotes():
        # return ["CN/22-23/00157"]
        cn_list = frappe.db.sql("""
            SELECT `tabSales Invoice`.`name` AS `name`, `tabSales Invoice`.`print_note_reference` AS `print_note_reference`
            FROM `tabSales Invoice`
            WHERE (`tabSales Invoice`.`creation` >= convert_tz('2023-03-20 00:00:00.000', 'Asia/Kolkata', @@session.time_zone)
            AND `tabSales Invoice`.`creation` < convert_tz('2023-03-22 00:00:00.000', 'Asia/Kolkata', @@session.time_zone) AND `tabSales Invoice`.`is_return` = 1)
            LIMIT 1048575
        """, as_list=True)
        si_flaten  = [si[0] for si in cn_list]
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
        cn_list = getCreditNotes()
        wrong_sis = []
        total_wrong_items = 0
        agg_cn_grand_total = 0
        agg_si_grand_total = 0
        for i, cn_name in enumerate(cn_list):
            T1 = datetime.datetime.now()
            print(f"[{cn_name}] {i+1}/{len(cn_list)} | Started")
            si = frappe.get_doc("Sales Invoice", cn_name)
            wrong_items = getWrongPriceSalesInvoiceItems(si)
            if len(wrong_items):
                print(f"[{cn_name}] {i+1}/{len(cn_list)} | Wrong items {len(wrong_items)}")
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
                            "qty": item.qty
                        } for item in wrong_items
                    ]
                }
                print(f"[{cn_name}] {i+1}/{len(cn_list)} | CN creation started")
                cn_res = create_sales_invoice(json.dumps(create_cn_request_obj))
                cn_name = cn_res.split(" | ")[0]
                cn_grand_total = float(cn_res.split(" | ")[2].split(" ")[-1])
                agg_cn_grand_total += cn_grand_total
                print(f"[{cn_name}] {i+1}/{len(cn_list)} | CN creation completed - [{cn_name}]: Grand Total = {cn_grand_total}")
                create_si_request_obj = {
                    "customer": si.customer,
                    "is_return": 0,
                    "source_warehouse": "Kadapa CDC - SK",
                    # "do_not_submit": True,
                    "print_note_reference": f"Wrong Price Adjust SI of [{si.name}]",
                    "items": [
                        {
                            "item_code": item.item_code,
                            "qty": -1*item.qty,
                            "rate": item.rate,
                            "retail_rate": item.retail_rate,
                        } for item in wrong_items
                    ]
                }
                print(f"[{cn_name}] {i+1}/{len(cn_list)} | Correct SI creation started")
                si_res = create_sales_invoice(json.dumps(create_si_request_obj))
                si_name = si_res.split(" | ")[0]
                si_grand_total = float(si_res.split(" | ")[2].split(" ")[-1])
                agg_si_grand_total += si_grand_total
                print(f"[{cn_name}] {i+1}/{len(cn_list)} | Correct SI creation completed - [{si_name}]: Grand Total = {si_grand_total}")
                frappe.db.commit()

            print(f"[{cn_name}] {i+1}/{len(cn_list)} | Completed in {datetime.datetime.now() - T1} Total time till now {datetime.datetime.now() - T}")
        
        print(f"Total time taken {datetime.datetime.now() - T}")
        print(f"Total wrong Sales Invoices = {len(wrong_sis)}")
        print(f"Total wrong items = {total_wrong_items}")
        print(f"Total CN Grand Total = {agg_cn_grand_total}")
        print(f"Total SI Grand Total = {agg_si_grand_total}")
        print(f"Total Excess Billed = {-1 * agg_cn_grand_total - agg_si_grand_total}")
    correctSalesInvoices()

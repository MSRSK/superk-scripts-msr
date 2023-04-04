import frappe

def main():
    def get_dcs_to_update_price():
        dc_list = frappe.db.sql("""
            SELECT `tabDelivery Note`.`name` AS `name`, `tabDelivery Note`.`creation` AS `creation`, `TabDocument Status Tracker`.`recon_completed` AS `TabDocument Status Tracker__recon_completed`
            FROM `tabDelivery Note`
            LEFT JOIN `tabDocument Status Tracker` `TabDocument Status Tracker` ON `tabDelivery Note`.`name` = `TabDocument Status Tracker`.`dc_no`
            WHERE (`tabDelivery Note`.`creation` >= convert_tz('2023-03-20 00:00:00.000', 'Asia/Kolkata', @@session.time_zone)
            AND `tabDelivery Note`.`creation` < convert_tz('2023-03-22 00:00:00.000', 'Asia/Kolkata', @@session.time_zone) AND `TabDocument Status Tracker`.`recon_completed` = 0 AND `tabDelivery Note`.`creation` < convert_tz('2023-03-21 15:03:12.433', '+05:30', @@session.time_zone))
            ORDER BY `tabDelivery Note`.`creation` DESC
            LIMIT 1048575
        """, as_dict=True)
        return list([dc.name for dc in dc_list])
        # return ["DC/22-23/16467"]


    def unsubmit_dc(dc_name):
        frappe.db.set_value("Delivery Note", dc_name, "docstatus", 0)


    def update_dc_prices():
        import datetime
        from superk.superk.doctype.delivery_challan_creation_tool.delivery_challan_creation_tool import update_dn_retail_variables, update_dn_row
        T = datetime.datetime.now()
        T1 = datetime.datetime.now()
        agg_pre_value = 0
        agg_current_value = 0
        dcs = get_dcs_to_update_price()
        print(f"Total dcs to update: {len(dcs)}")

        for i, dc in enumerate(dcs):
            T1 = datetime.datetime.now()
            print(f"[{dc}] {i+1}/{len(dcs)} Started")
            unsubmit_dc(dc)
            
            # update dc prices
            doc = frappe.get_doc("Delivery Note", dc)
            agg_pre_value += doc.grand_total
            grand_total = doc.grand_total
            items = doc.items.copy()
            # print("old items ",len(items))
            for item in items:
                doc.remove(item)
            # print("old items ",len(items), "new items ", len(doc.items))
            for row in items:
                new_row = doc.append("items")
                new_row = update_dn_row(new_row, row.item_code, row.qty, row.container, doc.customer, row.item_request)
                new_row.gst_percentage = frappe.db.get_value("Item", row.item_code, "gst_percentage")
                if new_row.rate != row.rate or new_row.retail_rate != row.retail_rate:
                    print(f"[{dc}] {i+1}/{len(dcs)} log | {row.item_code} {row.qty} {new_row.rate}/{row.rate} | {new_row.retail_rate}/{row.retail_rate}")
            
            doc.run_method("set_missing_values")
            doc.run_method("calculate_taxes_and_totals")
            # print(len(doc.items), len(items))
            # print("----------")
            # for i, new_row in enumerate(doc.items):
            #     row = items[i]
            #     if new_row.rate != row.rate or new_row.retail_rate != row.retail_rate:
            #         print(f"new [{dc}] {i+1}/{len(dcs)} log | {new_row.item_code}/{row.item_code} {new_row.qty}/{row.qty} {new_row.rate}/{row.rate} | {new_row.retail_rate}/{row.retail_rate}")

            doc = update_dn_retail_variables(doc)
            
            doc.save()
            doc.submit()
            agg_current_value += doc.grand_total
            print(f"[{dc}] {i+1}/{len(dcs)} Submitted | grand_total updated from {grand_total} -> {doc.grand_total}")
            frappe.db.commit()
            print(f"[{dc}] {i+1}/{len(dcs)} Commited | Time taken: {datetime.datetime.now()-T1}s Total time: {datetime.datetime.now()-T}s")
            print("------------------------------------------------------------------")
        
        print(f"Total time taken: {datetime.datetime.now()-T}s")
        print(f"Total value before: {agg_pre_value} Total value after: {agg_current_value} || delta: {agg_pre_value-agg_current_value}")


    
    update_dc_prices()
    
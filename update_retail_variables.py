import datetime
import frappe

def get_si_list():
    #================================ Change the filters here ================================
    list = frappe.db.get_all("Sales Invoice", fields=["name"], filters={"is_return": 0, "docstatus": 1, "name": "SI/22-23/03472"}, order_by="name asc")
    #=========================================================================================
    print(f"Total {len(list)} Invoices found")
    return list


def update_retail_variables(si_name):
    doc = frappe.get_doc("Sales Invoice", si_name)
    try:
        retail_net_total = 0.0
        retail_grand_total = 0.0
        retail_total_taxes_and_charges = 0.0

        for item in doc.items:
            # if not item.retail_rate:
            #     frappe.throw(f"[{si_name}] No retail rate in the item {item.idx}")
            # if not item.gst_percentage:
            #     frappe.throw(f"[{si_name}] No gst percentage in the item {item.idx}")
            item.retail_net_rate = (
                item.retail_rate * 100) / (100 + item.gst_percentage)
            item.retail_net_amount = item.retail_net_rate * item.qty
            item.gst_amount = item.amount - item.net_amount

            retail_net_total += item.retail_net_amount
            retail_total_taxes_and_charges += item.gst_amount
            retail_grand_total += item.retail_rate*item.qty
        
        if doc.retail_net_total != retail_net_total:
            doc.retail_net_total = retail_net_total

        if doc.retail_net_total != retail_net_total:
            doc.retail_net_total = retail_net_total
        
        if doc.retail_total_taxes_and_charges != retail_total_taxes_and_charges:
            doc.retail_total_taxes_and_charges = retail_total_taxes_and_charges

        if doc.retail_discount_amount != (doc.net_total - retail_net_total):
            doc.retail_discount_amount = doc.net_total - retail_net_total

        if doc.retail_grand_total != retail_grand_total:
            doc.retail_grand_total = retail_grand_total
        
        doc.save()
        return True
    except Exception as e:
        traceback = frappe.get_traceback()
        print(f"[{si_name}] Error in update_retail_variables: {traceback}")
        return False

T = datetime.datetime.now()
si_list = get_si_list()

failed_sales_invoices = []

TN = datetime.datetime.now()
for i, si in enumerate(si_list):
    # if i < 4400:
    #     continue
    if i % 100 == 0 and i != 0:
        TP = datetime.datetime.now() - TN
        TN = datetime.datetime.now()
        print("Commiting...")
        frappe.db.commit()
        print("Commited")
        print(f"Total time: {TN - T}s, Time taken for latest 100: {TP}s")
    response = update_retail_variables(si["name"])
    if not response:
        failed_sales_invoices.append(si["name"])
    else:
        print(f"{i+1}/{len(si_list)} - {si['name']} Done")

print("Failed Sales Invoices: ", failed_sales_invoices)
frappe.db.commit()

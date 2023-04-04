
def main():
    import frappe
    from superk.api.create_sales_invoice import create_sales_invoice
    import json
    def get_stores_and_tcs_value():
        return [
            # { "customer": "SuperK - Baba Supermarket", "tcs": 691.03993 },
            { "customer": "SuperK - Daily Needs", "tcs": 4294.25429 },
            { "customer": "SuperK - Discount Mart", "tcs": 6375.08748 },
            { "customer": "SuperK - JS Mart", "tcs": 5351.23061 },
            { "customer": "SuperK - Movtech Mart", "tcs": 6671.92783 },
            { "customer": "SuperK - MTR Supermarket Tirupati", "tcs": 779.56366 },
            { "customer": "SuperK - Nandyala Mart", "tcs": 4017.51653 },
            { "customer": "SuperK - PS Super Mart Penagaluru", "tcs": 2391.69001 },
            { "customer": "SuperK - Siva Jyothi", "tcs": 11111.51503 },
            { "customer": "SuperK - SLN Prakash Nagar", "tcs": 4136.24058 },
            { "customer": "SuperK - SLV Supermarket DC", "tcs": 1231.44808 },
            { "customer": "SuperK - Sree Durga Lakshmi", "tcs": 4917.37349 },
            { "customer": "SuperK - Sri Hari", "tcs": 17428.025 },
            { "customer": "SuperK - Sri Lakshmi Narasimha", "tcs": 8573.68636 },
            { "customer": "SuperK - Sri Padmavathi Supermarket", "tcs": 3695.27795 },
            { "customer": "SuperK - Sri Shiridi Sai", "tcs": 2018.77261 },
            { "customer": "SuperK - Sri Venkateswara", "tcs": 4936.36365 },
            { "customer": "SuperK - TY Supermarket", "tcs": 5160.07722 },
            { "customer": "SuperK - V Mart", "tcs": 7716.75832 },
            { "customer": "SuperK - Venkata Padmaja", "tcs": 6976.47477 }
            ]



    def create_cns():
        stores_tcs = get_stores_and_tcs_value()
        for si in stores_tcs:
            si["tcs"] = -1 * si["tcs"]
            print(f"Creating CN for {si['customer']}")
            abb = frappe.db.get_value("Customer", si["customer"], "abbreviation")
            create_cn_request_obj = {
                "customer": si["customer"],
                "is_return": 1,
                "print_note_reference": f"TCS reversal FY24 {abb}",
                "source_warehouse": "Kadapa CDC - SK",
                "do_not_submit": True,
                "items": [
                    {
                        "item_code": "Paper Billing Roll - 1N-30",
                        "qty": -1
                    }
                ]
            }
            cn_res = create_sales_invoice(json.dumps(create_cn_request_obj))
            cn_name = cn_res.split(" | ")[0]
            print(f"CN created - [{cn_name}]")

            cn_doc = frappe.get_doc("Sales Invoice", cn_name)
            cn_doc.tcs = si["tcs"]
            cn_doc.grand_total_new = cn_doc.tcs + cn_doc.grand_total
            cn_doc.rounded_grand_total = round(cn_doc.grand_total_new)
            cn_doc.retail_grand_total = cn_doc.retail_grand_total + cn_doc.tcs
            cn_doc.save()
            cn_doc.submit()
            print(f"CN submitted - [{cn_name}] | TCS = {si['tcs']} | Grand Total = {cn_doc.grand_total_new} | Retail Grand Total = {cn_doc.retail_grand_total}")
            frappe.db.commit()

    create_cns()



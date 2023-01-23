import frappe


def correct_si_inventory():
    from superk.api.inventory_update import BinUpdate
    import csv
    class CorrectInventory(BinUpdate):
        def get_update_objects(self, bin_doc_data, item_qty_dict):

            update_bin_table_list = []
            columns_to_update = ["sk_in_shelf"]

            for row in bin_doc_data:
                update_bin_table_list.append({
                    "binDocName": row['name'],
                    "updateQuantities":
                    {
                        "sk_in_shelf": f"- {item_qty_dict[row['item_code']]}",
                    }
                })

            return columns_to_update, update_bin_table_list
    def get_reversal_items_dict():
        import csv
        file = open('reversal_items_final.csv')
        csvreader = csv.reader(file)
        header = []
        header = next(csvreader)
        reversal_items = {}
        for row in csvreader:
            reversal_items[row[0]] = int(row[1])

        print(reversal_items)

        return reversal_items
    logger = frappe.logger("cw-inventory-update", allow_site=True,
                        file_count=0, max_size=100_000)
    logger.info(f"Correction reversal because Discrepancy was captured already")

    warehouse = "Ready to Pick | Kadapa CDC - SK"
    voucher_type = "Correction Reversal"
    voucher_doc = "Correction Reversal"
    reversal_items_dict = get_reversal_items_dict()

    try:
        items = []
        item_dict = {}

        for item_code in reversal_items_dict.keys():
            item_dict = {"item_code": item_code,
                        "qty": reversal_items_dict[item_code]}
            items.append(item_dict.copy())
        
        

        correct_inventory = CorrectInventory()
        correct_inventory.process_inventory_update(
            warehouse, items, voucher_type, voucher_doc)
        print(warehouse, items, voucher_type, voucher_doc)

        print("========================")
        print(f"Completed Item Count {len(items)}")

    except:
        logger.error(f"Error occured while Correction reversal")
        logger.error(frappe.get_traceback())
    frappe.db.commit()

correct_si_inventory()
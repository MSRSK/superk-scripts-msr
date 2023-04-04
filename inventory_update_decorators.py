import frappe

def main():
    def sk_inventory_update(operation, field):
        from superk.api.inventory_update import BinUpdate
        def actual_decorator(func):
            def inner(*args, **kwargs):
                allowed_operations = ["+", "-"]
                allowed_fields = [
                    "sk_inwarding",
                    "sk_putaway_in_progress",
                    "sk_hold_reverse_putaway_qty",
                    "sk_in_shelf",
                    "sk_reserved",
                    "sk_picked",
                    "sk_out_for_delivery",
                    "discrepancy"
                    ]
                
                if operation not in allowed_operations:
                    frappe.throw(f"Operation {operation} not allowed should be one of {', '.join(allowed_operations)}")
                if field not in allowed_fields:
                    frappe.throw(f"Field {field} not allowed should be one of {', '.join(allowed_fields)}")
                
                if "field_operation" in kwargs:
                    kwargs["field_operation"][field] = operation
                else:
                    kwargs["field_operation"] = {
                        field: operation
                    }

                class CorrectInventory(BinUpdate):
                    def get_update_objects(self, bin_doc_data, item_qty_dict):
                        update_bin_table_list = []
                        columns_to_update = []
                        update_quantities = {}
                        

                        for row in bin_doc_data:
                            for field in kwargs["field_operation"].keys():
                                columns_to_update.append(field)
                                update_quantities[field] = f"{kwargs['field_operation'][field]} {item_qty_dict[row['item_code']]}"
                            update_bin_table_list.append({
                                "binDocName": row['name'],
                                "updateQuantities": update_quantities
                            })
                        
                        print("update_bin_table_list", update_bin_table_list)
                        print("columns_to_update", columns_to_update)
                        return columns_to_update, update_bin_table_list
                kwargs["CorrectInventory"] = CorrectInventory()
                return func(*args, **kwargs)
            return inner
        return actual_decorator


    # ================================ ADD Decorators here ================================#
    @sk_inventory_update("-", "sk_out_for_delivery")
    @sk_inventory_update("+", "sk_picked")
    # ======================================================================================#
    def process_inventory(*args, **kwargs):
        correctInventory = kwargs["CorrectInventory"]
        print("CorrectInventory", correctInventory)
        print("kwargs", kwargs)
        correctInventory.process_inventory_update(kwargs["warehouse"], kwargs["items"], kwargs["voucher_type"], kwargs["voucher_doc"])
        frappe.db.commit()

    #======================== Implement Your get_items() ========================#
    def get_items():
        # items format
        # qty should be positive

        # return [
        #     {
        #         "item_code": "SK-0001",
        #         "qty": 1
        #     }
        # ]
        return [{
            "item_code": "8901242609209-10",
            "qty": 10
        }]
    #=============================================================================#
    
    process_inventory(**{
        "items": get_items(),
    #======================== Add You Constants ========================#
    #=============== This will be seen in inventory logs ===============#
        "warehouse":"Ready to Pick | Kadapa CDC - SK",
        "voucher_type": "<voucher_type>",
        "voucher_doc": "<voucher_doc>"
    #===================================================================#
    })
    print("Inventory Updated Successfully!")

main()
    
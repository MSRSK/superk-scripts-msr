def update_inventory():
    import frappe
    import boto3
    import json
    from decimal import Decimal

    dynamo_db = boto3.resource('dynamodb', region_name = 'ap-south-1')
    inventory_table = dynamo_db.Table('Inventory')

    bin_list = frappe.db.get_list("Bin", filters={"warehouse": "Ready to Pick | Kadapa CDC - SK"}, fields=["name", "item_code", "sk_in_shelf", "sk_reserved"])
    print(f"bin_list = {len(bin_list)}")
    bin_batch = []
    for i, bin in enumerate(bin_list):
        bin_batch.append(bin)
        if (i + 1) % 25 == 0 or i == len(bin_list) - 1:
            # logger.info(f"Syncing inventory in dynamodb for {json.dumps(items)}")
            items = json.loads(json.dumps(bin_batch), parse_float=Decimal)

            try:
                with inventory_table.batch_writer() as writer:
                    for item in items:
                        variant_id = frappe.db.get_value("Item", item["item_code"], "variant_id_sp")
                        if variant_id:
                            writer.put_item(
                                    Item={
                                        "variantId": variant_id,
                                        "warehouse": "Kadapa CDC - SK",
                                        "itemCode": item["item_code"],
                                        "erpId": item["name"],
                                        "availableQty": item["sk_in_shelf"],
                                        "reservedQty": item["sk_reserved"]
                                    })
                        else:
                            print(f"variant_id not found for {item['item_code']}")
                # logger.info("Loaded data into table %s.", inventory_table.name)
            except Exception as e:
                traceback = frappe.get_traceback()
                print(traceback)
                # logger.exception(f"Couldn't sync data into table {inventory_table.name}, traceback: {traceback}")
            print(f"synced {i + 1} bins")
            bin_batch = []
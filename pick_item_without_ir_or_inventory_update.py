import frappe

def pick_item(container, item, requested_product_code, item_name, qty, destination_type, destination, destination_name, picked, item_request, container_status, user, picking_status, number_of_packages, express_checkout, source_warehouse, sk_block, item_mrp=0, session=None, reason=None):
    # uuid = uuid4()

    # logger.info(f"UUID: {uuid} Parameters passed to pick_item container {container}, item {item}, requested_product_code {requested_product_code}, item_name {item_name},qty {qty}, destination_type {destination_type}, destination {destination}, \
    #                 destination_name {destination_name}, picked {picked}, item_request {item_request},container_status {container_status},user {user}, \
    #                 picking_status {picking_status},number_of_packages {number_of_packages},express_checkout {express_checkout},source_warehouse {source_warehouse}, item_mrp {item_mrp},session {session},reason {reason}")

    container_doc = frappe.get_doc("Container", container)
    ir_doc = frappe.get_doc("Item Request", item_request)
    # if ir_doc.picking_status != "Picking Requested":
    #     frappe.throw(f"This Item Request is already in {ir_doc.picking_status} status. \
    #         Picking is allowed only when the Item Request is in Picking Requested status.")

    if(container_doc.destination):
        if(container_doc.destination != ir_doc.destination):
            # logger.info(
            #     f"UUID: {uuid} Stopped from using a container that is meant for another destination. IR destination is {ir_doc.destination} & Container destination is {container_doc.destination}  ")
            frappe.throw(
                "The container is being used for another destination : " + container_doc.destination)
    if(container_doc.sk_block):
        if(container_doc.sk_block != sk_block):
            # logger.info(
            #     f"UUID: {uuid} Stopped from using a container that is meant for another Block. {sk_block} & Container Block is {container_doc.sk_block}")
            frappe.throw(
                "The container is being used for another Block : " + container_doc.sk_block)
    
    # Check if template is being picked
    item_doc = frappe.get_doc("Item", item)
    if(item_doc.has_variants == 1):
        frappe.throw(
            "The picked item is a template. Only variants can be picked. Please contact your supervisor")
    
    #Check if a variant of another item(apart from requested) is being picked
    if(item_doc.variant_of != requested_product_code):
        frappe.throw(
            "The picked item is a variant of product that is different from requested product.\
             This is not allowed. Please contact your supervisor")


    if not any(d.item_request_ref == item_request for d in container_doc.containers):

        row = container_doc.append("containers")
        row.item = str(item)
        row.item_name = str(item_name)
        row.qty = float(picked)
        row.requested_qty = float(qty)
        row.requested_item_code = requested_product_code
        row.item_request_ref = item_request
        row.number_of_packages = number_of_packages
        row.second_check_mrp = item_mrp
        if(express_checkout == '1'):
            row.verified_qty = float(picked)
    else:
        for item in container_doc.containers:
            if item.item_request_ref == item_request:
                item.qty = float(qty)

    container_doc.session = session
    container_doc.availability_status = container_status
    container_doc.destination_type = destination_type
    container_doc.destination = destination
    container_doc.destination_name = destination_name
    container_doc.picker = user
    container_doc.source_warehouse = source_warehouse
    container_doc.sk_block = sk_block

    container_doc.save(ignore_permissions=True)

    # item_request_doc = frappe.get_doc("Item Request", item_request)

    # item_request_doc.picking_status = picking_status
    # item_request_doc.picked_item = item
    # item_request_doc.picked_qty = picked
    # item_request_doc.item_mrp = item_mrp if item_mrp else 0
    # item_request_doc.picker = user
    # item_request_doc.container_number = container
    # item_request_doc.picked_time = frappe.utils.now()
    # item_request_doc.number_of_packages = number_of_packages
    # if reason:
    #     item_request_doc.first_check_reason = str(reason)

    # if(express_checkout == '1'):
    #     item_request_doc.picking_status = "Second Check Done"
    #     item_request_doc.second_check_mrp = item_mrp if item_mrp else 0
    #     item_request_doc.second_check_user = user
    #     item_request_doc.sec_check_container = container
    #     item_request_doc.sec_check_time = frappe.utils.now()
    #     item_request_doc.s_check_qty = picked
    #     item_request_doc.second_check_item_code = item
    #     if reason:
    #         item_request_doc.second_check_reason == str(reason)
    # logger.info(
    #     f"IR {item_request_doc.name} is being picked by {frappe.session.user} Express checkout? -  {express_checkout} ")
    # item_request_doc.save(ignore_permissions=True)
    # picking_done_inventory_update(item_request)


pick_item("N833",
            "8906080602184-10",
            "8906080602184",
            "Paper Boat Pulpy Orange - 125ml",
            35,
            "Customer",
            "SuperK - Sri Shiridi Sai",
            "SuperK - Sri Shiridi Sai",
            35,
            "a195dd32b194",
            "Picking in progress",
            "maarimalli58@gmail.com",
            "Picked up",
            None,
            False,
            "Kadapa CDC - SK",
            "Block - CDC",
            "10",
            "APP-ADHOC",
            reason=None
        )
# pick_item("C001",
#             "8901512540409-10",
#             "8901512540409",
#             "ACT II Chilli Surprise - 30g+11g",
#             35,
#             "Customer",
#             "SuperK - Siva Jyothi",
#             "SuperK - Siva Jyothi",
#             35,
#             "7f7881900c",
#             "Picking in progress",
#             "sujith@superk.in",
#             "Picked up",
#             None,
#             False,
#             "Kadapa CDC - SK",
#             "Block CDC",
#             "10",
#             "APP-ADHOC",
#             reason=None
#         )
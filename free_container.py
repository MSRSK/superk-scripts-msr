import frappe

def free_container(document, doctype):
    from superk.superk.doctype.container.container import remove_assignment
    container_doc = frappe.get_doc(doctype, document)

    for container in container_doc.containers:
        frappe.db.delete('Container Items', container.name)

    frappe.db.set_value(doctype, document, {
        'categories_concatenated': None,
        'picker': None,
        'destination_type': None,
        'destination': None,
        'destination_name': None,
        'source_warehouse': None,
        'availability_status': 'Free',
        'is_adhoc': 'No',
        'session': None,
        'container_weight': 0,
        'sk_block': None,
    })

    remove_assignment(document)

free_container("N833", "Container")
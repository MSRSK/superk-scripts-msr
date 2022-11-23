import frappe

issues_list = frappe.db.get_list("Issue", filters={"raised_by": "spa_admin@superk.in"})
print(f"{len(issues_list)} issues found")

processed_issues = ["ISS-2022-13233", "ISS-2022-13322", "ISS-2022-11189", "ISS-2022-11614", "ISS-2022-10564", "ISS-2022-11187"]

for i, issue in enumerate(issues_list):
    if issue.name in processed_issues:
        continue
    issue_doc = frappe.get_doc("Issue", issue.name)
    barcode = frappe.db.get_value("Item", {"product_id_sp": issue_doc.product_id}, "variant_of")

    if barcode:
        issue_doc.subject = f"[{barcode}] {issue_doc.subject}"
        issue_doc.save(ignore_permissions=True)
    
    if i % 50 == 0 and i > 0:
        print("Commiting....")
        frappe.db.commit()
        print("Commited")
    print(f"{i + 1}/{len(issues_list)} Updated {issue.name}")
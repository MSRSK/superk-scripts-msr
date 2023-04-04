from uuid import uuid4
import frappe

logger = frappe.logger("cw-inventory-update", allow_site=True,
                file_count=0, max_size=100_000)


def correct_putaway_inventory(doc_name, uuid=None):



    logger.info(f"{uuid} Pushing putaway inventory for putaway {doc_name}")
    print(f"{uuid} Pushing putaway inventory for putaway {doc_name}")
    from superk.api.inventory_update import BinUpdate
    class CorrectInventory(BinUpdate):
        def get_update_objects(self, bin_doc_data, item_qty_dict):

            update_bin_table_list = []
            columns_to_update = ["sk_in_shelf"]

            for row in bin_doc_data:
                update_bin_table_list.append({
                    "binDocName": row['name'],
                    "updateQuantities":
                    {
                        "sk_in_shelf": f"+ {item_qty_dict[row['item_code']]}",
                    }
                })

            return columns_to_update, update_bin_table_list

#++==================== ================ ================
    def get_reversal_items_dict():
        items = frappe.db.get_list("Putaway Purchase Receipt Item", filters={"parent": doc_name}, fields=["item_code", "qty"])
        logger.info(f"{uuid} Putaway items length {len(items)}")
        print(f"{uuid} Putaway items length {items}")
        return items

    warehouse = "Ready to Pick | Kadapa CDC - SK"
    voucher_type = "Putaway Purchase Receipt"
    voucher_doc = doc_name
    reversal_items_dict = get_reversal_items_dict()

    try:
        items = []
        item_dict = {}

        for item in reversal_items_dict:
            item_dict = {"item_code": item.item_code,
                        "qty": item.qty}
            items.append(item_dict.copy())
        
        
        logger.info(f"{uuid} Processing Inventory Update")
        print(f"{uuid} Processing Inventory Update")
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


def correct_putaway():
    docs = ['c6fbbf744548','564acf63cb2f','b8188fa551d7','ffc08cc77c33','f9e54437c9f9','d1336a5d6ddc','445461086d87','8e24ee03eb4e','2b582294978f','393a00eaaad0','424b42f699b8','71d693e327f8','959fea4bf24e','d54294bfc6e2','4c48cb050051','5f308c6d29b3','b6b064e854a1','773864b88439','9daf1faee1ce','1a44072b3f9c','cbb96b67a843','a77c5f6ce4db','9a2e2402d31f','4255c6a22e72','a435104176bb','ed15ba78be41','c3fb89b1354a','9d4344fa3613','54f17f0b7820','6a6931f813dd','73f856083062','16c51e0ec1b3','08730df4a303','8e2cf8e1c52d','d94de49e2be3','a0eea49db61d','ac8818468816','accd5b8341a5','b748fccf40b7','7b05a3f9c20d','878cd4f0aee1','f80bcfaa53c0','26ac1cb82ddd','02b1daf993ee','bc7aa6145fd1','3068f960ad13','ff07ff81eb52','72f59c044ff3','70d7b69c8306','23c2ceaf4aad','3a399810e2d3','0c9bfc491097','05d6287598a3','1265cab72d09','e6e2daf24f8b','b96dc6303eaf','0992e6c833b4','bfc27629d1a8','f8dfcde3b18c','f3d218135895','5bcc1785052e','b09882277285','96d06b655457','c3707e1a213b','83879018fdcd','a57302a9d635','7afed42ff5c1','01f6bd5b514a','d3dd2030439c','e49c84971b34','ec822df967db','6be3e2d305c1','5b9356802acd','50d2f16d4c82','bb2600c774c3','231b38c5205a','cf7981fdae7a','2ac88d3e9539','9f12128b6758','1eea4054c856','48251e3d51de','4f9506ebbbfa','f532a7204a3b','2f560832fe44','443587320140','49733af2abc3','bc4749e0cf7a','c68bffe40c65','81ed2c997a99','898f0f273291','d76b12d4d40c','474a2773b14b','f8a90ccaa53a','ff2c9e94898b','adb744eaf7ea','44fcf8fd9a75','bbe6c749cf1a','71117018b75c','7c66f3447f5d','a52e5de703c9','0dad73f24a1d','e520e3c28ca0','e7147758ff1d','4c5b54745a5f','6604dac7ac0a','7e336843e290','33bacd5e51f3','426228a29ccc','4c866b821cd4','a3b570e0f1d5','6886b7b29602','6bd113b96bca','1f6af10de8ff','dcc7e2112c9b','991da0b009b9','eea7a8bd6083','4d35bd98c03d','f23e909b7423','263352bc76ad','84afaa88f317','4950865e963e','a0514cd2b634','f2788af4f4bc','b8dd8d603a36','8d3cfa24fc9d','a7c8bea5fac0','7554faae4a2c','c1c4f7598c5a','53800bced791','f662fef2759f','56201b33f9dd','a7fa694abb58','6e326b2a7ee7','7e7c2baa887b','d1051ee185b6','3e9b5e089288','afd4cb4fbb87','5f4d0706e762','067f882d34ac','6f8effb73d25','c5e1308667a7','4e209c40764e','f8796ef90780','c8eef83f5f69','cb7dab9bb537','35ee679b1726','8eeff22d0fc7','f47319f766dc','b9b4bdc5e984','147b6b7d3ea1','38cb5376fd09','3b90bfb014d8','e925f2dbb800','f75a946bca5d','a6e35146863c','09e429b927fc','b635a184a761','1558f50ec6ff','bd093e2907fe','1d520a109a4d','cb20d3648c7d','82757694ad04','347bc2e3a2ac','8ba2adea3fd5','dc054bf25120','40e55b201457','9b89f322c28e','4dd5c01a1c6c','a959ec21cfd2','bd0c72b8ad0b','73f941d4a300','8169ef98633d','d178028b88f6','8845a864988d','e2425261c3a2','59697588008a','5dd5b9daad69','b71deaf49df2','6b95a948c104','6d29ca98d1c2','70510bf88378','bd16d961bc43','c3f9825ef375','382d3ef9e893','8d44f967147c','daeb5e23236e','ebe8e6980327','aa65b50fc959','1037a3726d94','170082d12028','786f48ce0202','823e422a854c','86cec42dfb11','37509b76acdd','8b564ca4bbc6','d8ad0591344a','db6d6d2d1147','8fa9d2c6ec0b','9b77766e9d50','9ffcaa120382','5b0dc1c2c7bd','ba9676e021e1','6e528e07aca4','c0e207d6dd78','7620be871a22','78e853e1c784','c4717df65c79','806e6426e68f','e100f401bfe4','e4f7a62e68da','a297e481303f','5078c247f917','56827b3d5faf','5943ba2d0685','08f6ffce73d1','b3907558ec51','6c096fefb927','ba970f64e704','c14c28f56823','c3f2617abe39','79a88ce151dc','c831ba7d884f','cb26f0677bd4','85e6586c7d5a','34b47ba1a1e6','901fdd114737','93cbbaf49959','460e604bf5c2','a29f838b33fd','01c8804a4318','0756b774602a','092f7a0b2e4f','b48875a419f0','0c154a6e03e7','0e45cdd920a7','1bc49f7edc3a','2a38f09bcae6','2fedad416e43','8399dfe4b408','d2614b88fc6f','3e00cdd08327','dc7654aa8f67','908940a306e3','418f6e4777fe','e47482530949','94bbcdac0693','e6d27665adf8','e72bd66bd1c9','9b70f171a679','edf56c3fd0ab','9fe71b2016fc','a632521a6f46','57650fa7c81e','596c93d3f02e','5c0fbd1e085f','07f2c959725c','b5beb4d5eba1','bbe6a6231723','75d0591a9776','c358ce72b68e','24a06565c10d','ca02174e61d6','34470154cbf0','42379a57b6bb','956fcfe67f63','439be599153a','e6da0b0adac6','4c3e06e82c7f','a270614490cd','f495c07899a9','54824a6c3fcc','a85f22fb8aa5','58deb8ddadcb','aaabf7ec9047','5c81e1b44475','b28c756cc44e','0ca26a9ff5bf','1fb78b9f954f','ce5d9b34fb6d','3763ad0e677a','8fc18cd26c10','92c5b0adccf1','959157887c83','43ae47580421','456a45ce1f51','ea1f03af1e77','9f6c76179126','f00f2b0f0ae8','5496c784e9f5','0164ab98f020','5b2c937f23c3','b0564343b496','5cc3cef17656','61177b908c15','0a9bf33d2fa1','6b6e406fc1d2','12f93de48a9a','bc2e467fda38','71ca1fe3e2d7','1baf25a3a787','7676155c7237','21bba1585f1a','7ce15da502ca','324756a2dd39','ce90f7fd0a57','3817c46266ce','d606363b4a9b','d9e7bb7be1d3','8ece093664da','3c4c9cc0186f','dc5db6d19917','41198f89845c','eb5b2704aff1','ed881449238f','f0a73b0c8a0a','f566a60299f0','56cbc0274eba','a9e1b042fa66']
    # docs = ['2359e3e740e3']
    for i, doc in enumerate(docs):
        uuid = uuid4()
        correct_putaway_inventory(doc, uuid)
        print(f"Corrected {i+1}/{len(docs)}")
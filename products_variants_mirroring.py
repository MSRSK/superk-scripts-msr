from google.cloud import bigquery
from google.oauth2 import service_account
import boto3
import requests
import json
import csv

# ERP_URL = "https://gamma.superk.in"
ERP_URL = "https://erp.superk.in"

GET_ACCESS_TOKEN_URL = ERP_URL + "/api/method/superk.superk.utils.get_spa_access_token"

# ERP_HEADER = {"Authorization": "token 9264f4b80081cc0:d7db3e13a6e8e12"} # for gamma.superk.in
ERP_HEADER = {"Authorization": "token b50ef12084e4ea2:b3647be6cce8df7"} #erp.superk.in

response = requests.request("GET", url=GET_ACCESS_TOKEN_URL, headers=ERP_HEADER)
token = json.loads(response.text)["message"]
# print(token)
print("Received SPA token from erp")

# SPA_BASE_URL = "https://h4or6drgc8.execute-api.ap-south-1.amazonaws.com/gamma"
SPA_BASE_URL = "https://1kb5gewg21.execute-api.ap-south-1.amazonaws.com/prod"

SPA_HEADER = {"Authorization": token}

#<><><><><><><><><><><><><><><><><><><><>(Getting Data From BigQuery)<><><><><><><><><><><><><><><><><><><><>#

def test_bigquery_data():
	print("Getting data from BigQuery...")
	path_to_creds = "./key_bq.json"

	credentials = service_account.Credentials.from_service_account_file(
		path_to_creds, scopes=["https://www.googleapis.com/auth/cloud-platform"])
	bqclient = bigquery.Client(
		credentials=credentials, project=credentials.project_id)
	
	querystring = """
			SELECT * FROM `turing-audio-266016.pipeline_v3.calculated_rules`
	"""
	dataframe = bqclient.query(querystring).result(
	).to_dataframe().to_json(orient='records')
	data = json.loads(dataframe)
	print(f"Found {len(data)} items")
	print(data[0])
	return data

def get_bigquery_data(table):
	print("Getting data from BigQuery...")
	path_to_creds = "./key_bq.json"

	credentials = service_account.Credentials.from_service_account_file(
		path_to_creds, scopes=["https://www.googleapis.com/auth/cloud-platform"])
	bqclient = bigquery.Client(
		credentials=credentials, project=credentials.project_id)

	if table == "Products":
		print("Fetching Products from BigQuery")
		querystring = """
				SELECT alias_name AS name, "" AS description, alias AS erpId,
				IF(cat_2 IS NULL, ARRAY[], IF(cat_3 IS NULL, ARRAY[cat_2], ARRAY_CONCAT([cat_2, cat_3]))) AS categories,
				brands AS brand,
				FROM `turing-audio-266016.pipeline_v3.alias_info` 
				WHERE alias_name IS NOT NULL
				AND alias IS NOT NULL
			"""
	elif table == "Variants":
		print("Fetching Variants from BigQuery")
		querystring = """
				SELECT DISTINCT
				product_code AS barcode, mrp, 
				MAX(SPA_Bundle) OVER (PARTITION BY product_identifier) AS bundleQty,
				IF(mrp>350,
					GREATEST(MAX(SPA_Bundle) OVER (PARTITION BY product_identifier), FLOOR(IFNULL(max_qty, 0)/MAX(SPA_Bundle) OVER (PARTITION BY product_identifier)*MAX(SPA_Bundle) OVER (PARTITION BY product_identifier))),
					GREATEST(MAX(SPA_Bundle) OVER (PARTITION BY product_identifier)*3, FLOOR(IFNULL(max_qty, 0)/MAX(SPA_Bundle) OVER (PARTITION BY product_identifier)*MAX(SPA_Bundle) OVER (PARTITION BY product_identifier)))
				) AS maxQty,
				CONCAT(product_code, "-", mrp) AS erpId,
				product_identifier AS alias, 
				picking_category AS pickingCategory,
				selling_price AS sellingPrice,
				FROM (
				SELECT
					DISTINCT Product_Code, Product_identifier, picking_category, SPA_Bundle
					FROM `turing-audio-266016.pipeline_v3.product_details`
					WHERE Product_Code IS NOT NULL
						AND Product_identifier IS NOT NULL)
				LEFT JOIN (
				SELECT
					Product_Code, MRP, MAX(Selling_Price) AS Selling_Price
				FROM `turing-audio-266016.superk_data.Stock_Warehouse`
				WHERE Category IS NOT NULL
				GROUP BY 1, 2 )
				USING (Product_Code)
				LEFT JOIN (
				SELECT alias AS product_identifier, CEILING(MAX(GREATEST(Mx, Visibility))) AS Max_Qty
				FROM `turing-audio-266016.pipeline_v3.alias_replenishment`
				GROUP BY 1 )
				USING (product_identifier)
				WHERE CONCAT(product_code, "-", mrp) IS NOT NULL
				"""
	else:
		print("Only Products and Variants tables are supported")
		return

	dataframe = bqclient.query(querystring).result(
	).to_dataframe().to_json(orient='records')
	data = json.loads(dataframe)
	print(f"Found {len(data)} items")
	print(data[0])
	return data
#======================================================================================================#


#<><><><><><><><><><><><><><><><><><><><>(Getting Data From DynamoDB)<><><><><><><><><><><><><><><><><><><><>#
def get_dynamodb_data(table_name):
	print("Getting data from DynamoDB...")
	dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

	table = dynamodb.Table(table_name)
	print(f"Scanning {table_name} table...")
	response = table.scan()
	data = response['Items']
	print(f"Found {len(data)} items")

	while 'LastEvaluatedKey' in response:
		print(f"Scanning for more {table_name}...")
		response = table.scan(
			ExclusiveStartKey=response['LastEvaluatedKey'])
		print(f"{len(response['Items'])} items fetched")
		data += response['Items']

	print(data[0])
	print(f"Total {len(data)} {table_name} fetched")
	return data
#======================================================================================================#


def mirror_products_to_dynamodb():
	print("Mirroring Products to DynamoDB...")
	bigquery_products_list = get_bigquery_data("Products")
	dynamodb_products_list = get_dynamodb_data("Products")

	# converting to dict, erpId as key
	print("Converting to dict...")
	dynamodb_products_data_dict = {}
	for i, data in enumerate(dynamodb_products_list):
		if "erpId" in data.keys():
			dynamodb_products_data_dict[data['erpId']] = data
	print("Converted to dict")

	to_be_updated_to_dynamodb = []
	to_be_created_in_dynamodb = []

	print("Comparing...")
	for data in bigquery_products_list:
		# creating a list of products to be updated & created
		if data['erpId'] not in dynamodb_products_data_dict.keys():
			if data['erpId'] == "8901058867220":
				print("<>"*10)
				# print(dynamodb_products_data_dict[data['erpId']])
				# print()

			to_be_created_in_dynamodb.append(data)
		else:
			update = {}
			if dynamodb_products_data_dict[data['erpId']]['brand'] or data['brand']:
				if data['brand'] != dynamodb_products_data_dict[data['erpId']]['brand']:
					print(f"Brand changed from ..{dynamodb_products_data_dict[data['erpId']]['brand']}.. to ..{data['brand']}..")
				# print(f"Brand changed from {dynamodb_products_data_dict[data['erpId']]} to {data}")
					update['brand'] = data['brand']
			# if data['categories'] != dynamodb_products_data_dict[data['erpId']]['categories']:
			# 	print(f"Categories changed from ..{dynamodb_products_data_dict[data['erpId']]['categories']}.. to ..{data['categories']}..")
			# 	update['categories'] = data['categories']
			if data['name'] != dynamodb_products_data_dict[data['erpId']]['name']:
				print(f"Name changed from ..{dynamodb_products_data_dict[data['erpId']]['name']}.. to ..{data['name']}..")
				update['name'] = data['name']
			if update:
				update['id'] = dynamodb_products_data_dict[data['erpId']]['id']
				to_be_updated_to_dynamodb.append(update)

	print(f"Found {len(to_be_updated_to_dynamodb)} products to be updated")
	print(f"Found {len(to_be_created_in_dynamodb)} products to be created")

	create_products_in_dynamodb(to_be_created_in_dynamodb)
	update_products_in_dynamodb(to_be_updated_to_dynamodb)


def create_products_in_dynamodb(to_be_created_in_dynamodb):
	print("Creating products in DynamoDB...")
	for i, product in enumerate(to_be_created_in_dynamodb):
		print(f"Creating product {i+1}/{len(to_be_created_in_dynamodb)}")
		create_product(product)
	print("Products created")


def create_product(product):
	url = SPA_BASE_URL + "/product"

	payload = {
		"name": product["name"],
		"erpId": product["erpId"],
		"brand": product["brand"],
		"categories": product["categories"],
	}

	response = requests.request(
		"POST", url, headers=SPA_HEADER, data=json.dumps(payload))
	if response.status_code == 200:
		response = json.loads(response.text)
		print("Product created successfully")
	else:
		print("Error in creating product")


def update_products_in_dynamodb(to_be_updated_to_dynamodb):
	print("Updating products in DynamoDB...")
	for i, product in enumerate(to_be_updated_to_dynamodb):
		update_product(product)
		print(f"{i+1}/{len(to_be_updated_to_dynamodb)} products updated")
	print("All products updated")


def update_product(product):
	base_url = SPA_BASE_URL
	url = base_url + "/product/" + product['id']
	payload = {}

	if "brand" in product.keys():
		payload["brand"] = product["brand"]
	if "categories" in product.keys():
		payload["categories"] = product["categories"]
	if "name" in product.keys():
		payload["name"] = product["name"]
	if "showInAppList" in product.keys():
		payload["showInAppList"] = product["showInAppList"]
	if "showInApp" in product.keys():
		payload["showInApp"] = product["showInApp"]
	

	if payload:
		response = requests.request(
			"PUT", url, headers=SPA_HEADER, data=json.dumps(payload))
		if response.status_code == 200:
			print("Product updated")
		else:
			print("Error in updating product")


def mirror_variants_to_dynamodb():
	print("Mirroring Variants to DynamoDB...")
	bigquery_variants_list = get_bigquery_data("Variants")
	dynamodb_variants_list = get_dynamodb_data("Variants")
	dynamodb_products_list = get_dynamodb_data("Products")

	# converting to dict, erpId as key
	print("Converting variants to dict...")
	dynamodb_variants_data_dict = {}
	for data in dynamodb_variants_list:
		dynamodb_variants_data_dict[data['erpId']] = data

	# converting to dict, erpId as key
	print("Converting products to dict...")
	dynamodb_products_data_dict = {}
	for data in dynamodb_products_list:
		if "erpId" in data.keys():
			dynamodb_products_data_dict[data['erpId']] = data

	to_be_updated_to_dynamodb = []
	to_be_created_in_dynamodb = []

	print("Comparing...")
	barcode_c = 0
	bundleQty_c = 0
	maxQty_c = 0
	mrp_c = 0
	pickingCategory_c = 0
	for data in bigquery_variants_list:
		# creating a list of variants to be updated & created
		if data['erpId'] not in dynamodb_variants_data_dict.keys():
			variant = data.copy()
			print(f"Creating variant {variant['erpId']} and product {variant['alias']}")
			variant['productId'] = dynamodb_products_data_dict[data['alias']]['id']
			to_be_created_in_dynamodb.append(variant)
		else:
			update = {}
			if data['barcode'] != dynamodb_variants_data_dict[data['erpId']]['barcode']:
				barcode_c += 1
				update['barcode'] = data['barcode']
			if data['bundleQty'] != float(dynamodb_variants_data_dict[data['erpId']]['bundleQty']):
				bundleQty_c += 1
				update['bundleQty'] = data['bundleQty']
			if data['maxQty'] != float(dynamodb_variants_data_dict[data['erpId']]['maxQty']):
				maxQty_c += 1
				update['maxQty'] = data['maxQty']
			if data['mrp'] != float(dynamodb_variants_data_dict[data['erpId']]['mrp']):
				mrp_c += 1
				update['mrp'] = data['mrp']
			if "pickingCategory" in dynamodb_variants_data_dict[data['erpId']].keys():
				if data['pickingCategory'] != dynamodb_variants_data_dict[data['erpId']]['pickingCategory']:
					pickingCategory_c += 1
					update['pickingCategory'] = data['pickingCategory']
			else:
				update['pickingCategory'] = data['pickingCategory']
			if data['sellingPrice'] != float(dynamodb_variants_data_dict[data['erpId']]['sellingPrice']):
				update['sellingPrice'] = data['sellingPrice']

			if dynamodb_products_data_dict[data['alias']]['id'] != dynamodb_variants_data_dict[data['erpId']]['productId']:
				update['productId'] = dynamodb_products_data_dict[data['alias']]['id']
				update['oldProductId'] = dynamodb_variants_data_dict[data['erpId']]['productId']

			if update:
				print("<>"*15)
				print(dynamodb_variants_data_dict[data['erpId']])
				print(update)
				update['id'] = dynamodb_variants_data_dict[data['erpId']]['id']
				update['productId'] = dynamodb_products_data_dict[data['alias']]['id']
				to_be_updated_to_dynamodb.append(update)
	print(f"{barcode_c} variants updated barcode")
	print(f"{bundleQty_c} variants updated bundleQty")
	print(f"{maxQty_c} variants updated maxQty")
	print(f"{mrp_c} variants updated mrp")
	print(f"{pickingCategory_c} variants updated pickingCategory")
	print(f"Found {len(to_be_updated_to_dynamodb)} variants to be updated")
	print(f"Found {len(to_be_created_in_dynamodb)} variants to be created")

	create_variants_in_dynamodb(to_be_created_in_dynamodb)
	update_variants_in_dynamodb(to_be_updated_to_dynamodb)


def create_variants_in_dynamodb(to_be_created_in_dynamodb):
	print("Creating variants in DynamoDB...")
	for i, variant in enumerate(to_be_created_in_dynamodb):
		create_variant(variant)
		print(f"{i+1}/{len(to_be_created_in_dynamodb)} variants created")
	print("All variants created")


def create_variant(variant):
	url = SPA_BASE_URL + "/product/" + variant['productId'] + "/variant"

	payload = {
		"barcode": variant["barcode"],
		"mrp": variant["mrp"],
		"bundleQty": variant["bundleQty"],
		"maxQty": variant["maxQty"],
		"erpId": variant["erpId"],
		"productId": variant["productId"],
		"sellingPrice": variant["sellingPrice"],
	}

	response = requests.request(
		"POST", url, headers=SPA_HEADER, data=json.dumps(payload))
	if response.status_code == 200:
		response = json.loads(response.text)
		print("Variant created successfully")
		update_data_in_erp(response["erpId"],
						   response["productId"], response["id"])
	else:
		print(f"payload: {payload}")
		print("Error in creating Variant")


def update_variants_in_dynamodb(to_be_updated_to_dynamodb):
	print("Updating variants in DynamoDB...")
	for i, variant in enumerate(to_be_updated_to_dynamodb):
		print(f"{i+1}/{len(to_be_updated_to_dynamodb)} variants updated")
		update_variant(variant)
	print("Variants updated")


def update_variant(variant):
	if "oldProductId" in variant.keys():
		old_product_id = variant['oldProductId']
		del variant['oldProductId']
	else:
		old_product_id = variant['productId']
		del variant['productId']

	url = SPA_BASE_URL + "/product/" + \
		old_product_id + "/variant/" + variant['id']
	payload = {}

	if "barcode" in variant.keys():
		payload["barcode"] = variant["barcode"]
	if "bundleQty" in variant.keys():
		payload["bundleQty"] = variant["bundleQty"]
	if "maxQty" in variant.keys():
		payload["maxQty"] = variant["maxQty"]
	if "mrp" in variant.keys():
		payload["mrp"] = variant["mrp"]
	if "sellingPrice" in variant.keys():
		payload["sellingPrice"] = variant["sellingPrice"]
	if "productId" in variant.keys():
		payload["productId"] = variant["productId"]
	if "pickingCategory" in variant.keys():
		payload["pickingCategory"] = variant["pickingCategory"]

	if payload:
		response = requests.request(
			"PUT", url, headers=SPA_HEADER, data=json.dumps(payload))
		if response.status_code == 200:
			response = json.loads(response.text)
			if response["productId"] != old_product_id:
				update_data_in_erp(
					response["erpId"], response["productId"], response["id"])
			print("Variant updated")
		else:
			print(f"url: {url}")
			print(f"variant: {variant}")
			print(f"payload: {payload}")
			print("Error in updating Variant")
			print(response.text)


def update_data_in_erp(erp_id, product_id, variant_id):
	url = ERP_URL + "/api/resource/Item/" + erp_id
	payload = {
		"product_id_sp": product_id,
		"variant_id_sp": variant_id
	}

	response = requests.request(
		"PUT", url=url, headers=ERP_HEADER, data=json.dumps(payload))
	print(response.text)
	if response.status_code != 200:
		print(f"Error in updating data in ERP for variant {erp_id}")


def update_variant_with_blank_payload():
	dynamodb_variants_list = get_dynamodb_data("Variants")

	for i, variant in enumerate(dynamodb_variants_list):
		# url = SPA_BASE_URL + "/product/" + variant['productId'] + "/variant/" + variant['id']
		url = SPA_BASE_URL + "/product/" + "2c9e655a-1b72-4628-b9f7-8c4d885c4e86" + "/variant/" + "bd4a56bd-7f28-44e2-9dbc-6b024d78b003"
		payload = {}

		response = requests.request(
			"PUT", url=url, headers=SPA_HEADER, data=json.dumps(payload))
		print(f"{i+1}/{len(dynamodb_variants_list)} variants updated")
		if response.status_code != 200:
			print(response.text)
			print(f"Error in updating data in ERP for variant {variant['id']}")


def delete_order(store_id, order_id):
	token = "eyJraWQiOiJPR2lDWU92M2JoTnpvczUwcVwvVE5CVmJobk9VXC82SEJjSExVS0ZqcHYzUHM9IiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiJjOGIwODRpbGNlYTE3Y2JpY29vZTZhMnN1IiwidG9rZW5fdXNlIjoiYWNjZXNzIiwic2NvcGUiOiJlcnAuc3VwZXIuaW5cL2FkbWluX2FjY2VzcyIsImF1dGhfdGltZSI6MTY2MTM2NDg3OSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLmFwLXNvdXRoLTEuYW1hem9uYXdzLmNvbVwvYXAtc291dGgtMV9IaTM4QjA0V1QiLCJleHAiOjE2NjE0NTEyNzksImlhdCI6MTY2MTM2NDg3OSwidmVyc2lvbiI6MiwianRpIjoiYjY1Mzg2ZDAtNmU1MC00NDlkLWExNWMtMjhkN2RmMDMwN2VjIiwiY2xpZW50X2lkIjoiYzhiMDg0aWxjZWExN2NiaWNvb2U2YTJzdSJ9.azourEaY1eXOvDSulveso3CvAUW0E7yuGBr8xy_P4AlPsTuDr4rHaziU1QfFWMAUIdVAZALDWhpXtUmm3eNXJ68w0yE3SFcf3VOB_ZfJ83rkJPw5TH0MbCsMLx6JPfG-1y-G_FxNIWVq4Mhn4RsQbllFxnEzrFtVAwce2I2deSw8tZXcnhW6shsnO4hObRWZcp6JtMez-SwQVhAUuYs0z9n3T3_ryLcTUlo_GmAAopbsISk84hQnBR4-6WkeRds_Tbg1Uk4sKJDZPDJFsiNHUDaNQIY5La5yljV19oXtgwGq_ORySc_q2ejT4TGt5mMJqhlgqKrXrPhC6THdgTol7A"
	print(f"Delete order {order_id}")
	url = SPA_BASE_URL + "/store/" + store_id + "/order/" + order_id + "/items"
	print(url)

	payload = {
        "stockOrderType": "ARS"
	}
	print(json.dumps(payload))

	response = requests.request("DELETE", url, headers=SPA_HEADER, data=json.dumps(payload))

	if response.status_code == 200:
		print(response.text)
		response = json.loads(response.text)
		print("Order created successfully deleted")
	else:
		print(f"response: {response.text}")
		print("Error in creating product")

def delete_orders():
	orders = [
	{ 'store': '2eb1c231-182f-4ef5-9074-6aacac67d7c7',
	'order': '6a5b9ab4-a13c-4453-99a8-7353b31e5d2b' },
	{ 'store': 'b886c31e-0ecd-4998-9dc4-f932ae7b998e',
	'order': 'fa72c05d-4a1b-4842-ba36-a4abed09f50b' },
	{ 'store': 'ffc16909-15f6-4f07-96fd-b703f3b6b0cb',
	'order': '8781f373-57b6-41e0-a6e3-42b2f928d700' },
	{ 'store': '2730c5ad-cbce-4d9e-b948-0f651f66a308',
	'order': '9a2de4f3-00f7-4463-b7fe-22325ec529c3' },
	{ 'store': 'ee992f5b-f362-4d17-bea9-55dbdd7f1931',
	'order': '7a858c0e-6ca9-41a8-a629-de4d8121d7fb' },
	{ 'store': '2e3cdd85-5c24-4de8-bb16-d8d2bd34831e',
	'order': 'f225d8c9-2383-4b38-9002-d7bb76409124' },
	{ 'store': '6229ed29-2f7a-456f-b874-0618489ba8f0',
	'order': '299833b7-b794-4217-9bfd-5bdcaec2fde1' },
	{ 'store': '8317e9ce-6998-454a-a205-98dbf488026a',
	'order': 'daa71e47-a62c-4a59-8c27-9270ecab0ab3' },
	{ 'store': '42c16e61-f63d-4e7c-be5f-109c22dac635',
	'order': '236cdd9f-6b81-475e-80f5-1eb676baa5ec' },
	{ 'store': 'ec14fef5-b671-4cf8-8570-1d06f4eb85a2',
	'order': '239d91ed-cde5-4fd3-8662-5958705b040a' },
	{ 'store': 'ec411dab-630c-4502-8ba1-d79907e722b3',
	'order': '27a6e369-d358-4721-a390-e7c73a433857' },
	{ 'store': '3e3ca946-0aa8-4949-9d15-5bdf0e265d37',
	'order': '4a1965a6-1df9-4b72-ac63-d9ea899dae49' },
	{ 'store': '9398c482-804c-4728-a67d-c34f31f96603',
	'order': '37bfc946-543c-4479-a28f-b193839de859' },
	{ 'store': '93cc00a0-ec4a-4632-97e6-efd7fcbf61dd',
	'order': '488decdc-e908-42ea-95bd-d1f8ff5816d8' },
	{ 'store': '3823548e-69a3-47ce-89b4-d6fcd3f734c2',
	'order': '46f4e28f-8c0a-4739-a4b2-e5b0465517b0' },
	{ 'store': '8e6364ad-f96e-49d8-ad05-1a1821fe4faa',
	'order': 'f8dc2683-ae2f-4490-8a68-6350f6fc5c7a' },
	{ 'store': '4d43d94c-ae9e-49c1-8b40-c7b83378439f',
	'order': '713977a2-0acb-416f-8027-6d4444660d5a' },
	{ 'store': 'f2ad32ae-4de0-4a7a-83a2-c482f8cf2f30',
	'order': '32f8b456-c71a-4dcf-bc6b-bff0cfc2af1b' },
	{ 'store': 'a0bec7ea-4a34-40c8-836a-dc0044851978',
	'order': '64e11e2d-c22c-4988-b4da-2a6fbaf2c4ae' }
	]
	
	for i, order in enumerate(orders):
		# if i == 1:
		# 	break
		delete_order(order['store'], order['order'])
		print(f"{i+1}/{len(orders)} orders deleted")

def create_ars_order_items():
	import datetime
	T = datetime.datetime.now()
	T1 = datetime.datetime.now()
	file = open('order_items.csv')
	csvreader = csv.reader(file)
	header = []
	header = next(csvreader)
	order_items = []

	for row in csvreader:
		order_items.append({
			'store': row[0],
			'order_id': row[1],
			'product_id': row[2],
			'quantity': row[3],
		})
	
	no_stock_products = 11
	
	for i, order_item in enumerate(order_items):
		if i < 3668:
			print("="*20)
			print(f"{i+1}/{len(order_items)} Skip")
			print("="*20)
			continue
		if i % 100 == 0:
			print("="*20)
			print(f"Time taken for last 100 Order{datetime.datetime.now() - T1} time taken for {i} Order{datetime.datetime.now() - T}")
			print("="*20)
			T1 = datetime.datetime.now()
		# if i == 100:
		# 	break
		response = create_ars_order_item(order_item['store'], order_item['order_id'], order_item['product_id'], order_item['quantity'])
		if response == "No Stock at WH":
			no_stock_products += 1

		print(f"{i+1}/{len(order_items)} order items created")
	print(f"{no_stock_products} products with no stock")

	


def create_ars_order_item(store_id, order_id, product_id, quantity):
	url = f"{SPA_BASE_URL}/store/{store_id}/order/{order_id}/item/{product_id}"

	payload = {
		"requestedQuantity": quantity,
		"stockOrderType":"ARS"
	}

	response = requests.request("POST", url, headers=SPA_HEADER, data=json.dumps(payload))

	if response.status_code != 200:
		print(f"response: {response.text}")
		return "No Stock at WH"
	else:
		return response.json()

def update_product_visibility_variables():
	dynamodb_products_list = get_dynamodb_data("Products")

	for i, product in enumerate(dynamodb_products_list):
		update_product({"id": product['id'], "showInApp": True, "showInAppList": True})
		print(f"{i+1}/{len(dynamodb_products_list)} products updated")
		# if i > 10:
		# 	break

def create_users():
	import csv

	file = open('user_data.csv')
	csvreader = csv.reader(file)
	header = []
	header = next(csvreader)
	config = {}

	for row in csvreader:
		if row[2] in config.keys():
			config[row[2]]["stores"].append(row[0])
		else:
			config[row[2]] = {
				"name": row[1],
				"phone": row[2],
				"stores": [row[0]]
			}
	i = 0
	for key in config.keys():
		i += 1
		create_user(config[key]["name"], config[key]["phone"], config[key]["stores"])
		print(f"{i}/{len(config.keys())} users created")

def create_user(name, phone, stores):
	url = f"{SPA_BASE_URL}/user"

	payload = {
		"name": name,
		"phoneNumber": f"+91{phone}",
		"stores": stores,
		"role": "SAE"
	}

	response = requests.request("POST", url, headers=SPA_HEADER, data=json.dumps(payload))

	if response.status_code != 200:
		print(f"response: {response.text}")
	else:
		print(response.json())

#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>#
# get_bigquery_data("Products")
# get_dynamodb_data("Products")
# mirror_products_to_dynamodb()
# delete_orders()
# create_ars_order_items()
# mirror_variants_to_dynamodb()
# update_data_in_erp("8906108890104-20", "2322df", "213521sf")
# update_variant_with_blank_payload()
# update_product_visibility_variables()
# test_bigquery_data()
create_users()
#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>#
import boto3

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



def get_item_request_of_a_order_id(order_id):
    print("Getting item request of a order id...")
    
    all_order_items = get_dynamodb_data()

    filtered_order_items = list(filter(lambda x: x['order_id'] == order_id, all_order_items))

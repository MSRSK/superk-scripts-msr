import boto3
from datetime import datetime

def get_dynamodb_data(dynamodb, table_name):
	print("Getting data from DynamoDB...")

	# print(f"Scanning {table_name} table...")
	response = dynamodb.scan(TableName = table_name)
	data = response['Items']
	print(f"Found {len(data)} items")

	while 'LastEvaluatedKey' in response:
		# print(f"Scanning for more {table_name}...")
		response = dynamodb.scan(
			TableName = table_name,
			ExclusiveStartKey=response['LastEvaluatedKey'])
		data += response['Items']
		print(f"{len(data)} items fetched")

	print(f"Total {len(data)} {table_name} fetched")
	return data

def put_dynamodb_data(dynamodb, table_name, items):
	total_items_put = 0
	while items:
		# print(f"Writing {len(items)} items to {table_name}...")
		dynamodb.batch_write_item(
			RequestItems = {
				table_name: [
					{
						'PutRequest': {
							'Item': item
						}
					} for item in items[:25]
				]
			}
		)
		# print(f"Successfully written {len(items)} items to {table_name}")
		items = items[25:]
		total_items_put += 25
		if total_items_put % 1000 == 0:
			print(f"Total {total_items_put} items put")
	print(f"Total {total_items_put} items put")


gamma_dynamoDB = boto3.client(
	'dynamodb',
	region_name="ap-south-1",
	aws_access_key_id="ASIA4JXCZKIPO4L3YWHE",
	aws_secret_access_key="ysplOcf463KAh90HVjwubKhucbSHrx91Hdgl+hn2",
	aws_session_token="IQoJb3JpZ2luX2VjEAMaCmFwLXNvdXRoLTEiRzBFAiEAhwFtH4FbjCZIxXNBAkq20j8D0f3X3/tumKuJ2Pq65fgCICOqvGDRN9lKpGJuz20KP+SMZ06W4cxb1YsYyErzBOMoKpIDCNz//////////wEQABoMODQ1NTEwNDMxMjYyIgz88KgCfiAoOByHXnMq5gJ3NFHdx8bxTnhIsvEa4zNnCYqOeUvB50aw5knyNjoKpW/GEBb84OpogMzmmD2v+29I00VNa+qf+OyCRah4W3Sxi4oEImt/mpc5/bvL/tYNSeDPDlh7HD/mY93T8SIbl9Liegl1Qh496V97uELna6yUq/4aNQFaHbiKx+rrYD/rPlwhheJ1AWpSImUyxLNkvIuJDRx48WuObTqvWqPIrElPYsBNK3eSU7p6uehSfMxl2eFcsLJNh6kclMZGHM3jJzhps0X4oA1oYMrKsNkdObC/pDXLLfSjTcKQ4jiS5OWkZ/D0vG31wGUGI0vDojY2C1K2uNiNQSo4J4rmFnc90ZWYjx+z3xqnYv9zrMUXi5Stb+Sdaggw015Fdj45zW5LLK8PbfbAtit8XJBtzeAHSKA3hc7AqHNLnzpMWJ7Cz6HAP2VefDhAk5pI7n4PJIVqADxpkBQdy10hajvUIZ6GLbzsE9Ue2onEMMW/rKEGOqYBZ2jwe9MeRpwH6Lnap7A7qrvoVtvqJaW2Sm3UbawafDZ9iJe94qqq78cJI/57IGmjHJlzzm3YcJZfFol6Ssw4jGSBV8pWyUswf1uMFy3yxTtYxcWyo6x4bDYLQ7maPeXUFqI6eeWw56UkVuwPCCWHRHf26OIdwP2UOVv7KIJHIM1/C0HYAqq+dftOcOo4D5/7gORudHyUBTu/UzuZciq+QaKpI4al1A=="
)
beta_dynamoDB = boto3.client(
	'dynamodb',
	region_name="ap-south-1",
	aws_access_key_id="ASIAXRT6MV4IB7PMWPEX",
	aws_secret_access_key="o3mZBesrHFyPHMWnuLrCmJTc8wRfWTux4DQsurOH",
	aws_session_token="IQoJb3JpZ2luX2VjEAMaCmFwLXNvdXRoLTEiSDBGAiEAkUiX5tHLBBXnwhVYSR6kug7PXo0irxxV5q/N1p539FYCIQC6eyI17wy8DpoRwVPTnCuEvGHyj/AMDP5AYYTxFzIEyiqTAwjc//////////8BEAAaDDUxODg4MjM3MzM5MiIMz3EpRtJ8OKAmqO1yKucCOnU2Ibq1gS/WuMBpcHBOu1L52dlSdIs08coi4xtUY4TQ6F1BEwXPqBkGtk7pJ3akaMdJ0+AbQ9BKhQIlB6/1BgmFNuzJOG7b/KGFlDpEANOUnihcsf/tUeQ03I1jUK4QricqIh3w+UbXuitQILfMeDjLfW4hNZVJCBmKQzuq+FycaaAgDA/qOYwmbGJiSMe3zriAyqah2oOZWZhEZGzBZcZK8iFCbFxDvVYUQaZsKGrVMrZ23pfCOSIn6MidslltV1FijLg04/G3O9yL6NZ221+zom4EqT5FzN+/NgR+06nPFU+OuD+TIOrQ+LhfGSPe2EtQ5AqAmWcnymiphuLhVaq2dIMCPwhmyxnNdb+8kvMeW8qEBgscHWPNSfKGoozGfdNHpnu+gyEk4sJ6B7X30WreCKyUzFLFPsxtsCfSMDzYD9CHXDPT2ZV/3wwukbG5oAODG1wMHDhL0zF1fuQIeB8VhlYEQCowz7usoQY6pQGu0pS7j/2+j66felo33YZnGMn/vjDapBnxNvV3TpO1YRQcPkHqOWqI2eYgMIJPlJP5Qqeu90W8ya4mf1KD4qWxsZwxstIhfW4tbupUDxxpmSNKcRhC2nTErXwSsOo3679osoREp59w/NLM3EFs5gi0VNwBYuEHkEmuoA1c5yKfmZ6gkKuRG5K7xgC3RF8di4pblN73AfVL9cpFrrjukHmEsGP8d+E="
)

transfer_tables = beta_dynamoDB.list_tables()["TableNames"]
table_not_to_transfer = ["LocationProductPrice", "LocationProductPriceHistory", "StoreInvoiceProducts"]
transfer_tables = [ele for ele in transfer_tables if ele not in table_not_to_transfer]

print(transfer_tables)
print(f"Total tables: {len(transfer_tables)}")

actual_start_time = datetime.now()
get_log = []
put_log = []
for table_name in transfer_tables:
	startTime = datetime.now()
	print(f"===================== {table_name}(gamma) =====================")
	items = get_dynamodb_data(gamma_dynamoDB, table_name)
	print(len(items))
	print(f"Took {datetime.now() - startTime} to get {len(items)} items from {table_name}(gamma)")
	get_log.append(f"Took {datetime.now() - startTime} to get {len(items)} items from {table_name}(gamma)")
	startTime = datetime.now()

	print(f"===================== {table_name}(beta) =====================")
	put_dynamodb_data(beta_dynamoDB, table_name, items)
	print(f"Took {datetime.now() - startTime} to put {len(items)} items to {table_name}(beta)")
	put_log.append(f"Took {datetime.now() - startTime} to put {len(items)} items to {table_name}(beta)")
	print(f"total {datetime.now() - actual_start_time}")

print(get_log)
print(put_log)
print(f"total {datetime.now() - actual_start_time}")
import csv
import boto3

dynamodb = boto3.resource('dynamodb', region_name = 'ap-south-1')

table = dynamodb.Table('Variants')
with open('erp_variants_data.csv', 'w', encoding='UTF8') as f:
    print("Scaning table...")
    response = table.scan()
    data = response['Items']
    print(f"Found {len(data)} items")

    header = ["Item Code", "Product Id", "Variant Id"]
    writer = csv.writer(f)
    writer.writerow(header)

    for item in data:
        writer.writerow([item["erpId"], item["productId"], item["id"]])

    while 'LastEvaluatedKey' in response:
        print("Scanning for more...")
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        print(f"{len(response['Items'])} items fetched")
        for item in response['Items']:
            writer.writerow([item["erpId"], item["productId"], item["id"]])
        print("Writing to file completed")

print(data[0])
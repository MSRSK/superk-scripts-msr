import datetime
import requests
import frappe
import csv

file = open('Store Info.csv')
csvreader = csv.reader(file)
header = []
header = next(csvreader)
source_cordinates = []

for row in csvreader:
        source_cordinates.append({
            "lat": row[1],
            "lng": row[2],
            "location": row[0]
        })

dest_cordinates = source_cordinates.copy()

# source_cordinates = [
#     {
#         "lat": "17.380453104565554",
#         "lng": "78.4864471084659",
#         "location": "Siva Jyothi"
#     }
# ]

# dest_cordinates = [
#     {
#         "lat": "12.901645944737346",
#         "lng": "77.6514803973435",
#         "location": "Warehouse"
#     }
# ]

def get_distance_matrix(source_cordinates, dest_cordinates):
    T = datetime.datetime.now()
    T1 = datetime.datetime.now()
    with open('distance_matrix.csv', 'w', encoding='UTF8') as f:
        header = ["Source", "Destination", "Distance (in Km)"]
        writer = csv.writer(f)
        writer.writerow(header)
        for i, source in enumerate(source_cordinates):
            if i < 32:
                continue
            for j, dest in enumerate(dest_cordinates):
                if i == 32 and j < 44:
                    continue
                distance = get_distance(f"{source['lat']}, {source['lng']}", f"{dest['lat']}, {dest['lng']}")
                writer.writerow([source['location'], dest['location'], distance])
                print(f"{i}/{len(source_cordinates)} - {j}/{len(dest_cordinates)}")
                print(f"{source['location']} to {dest['location']} is {distance}")
                print("="*20)
            print(f"Time taken for {source['location']} source: {datetime.datetime.now() - T1}")
            T1 = datetime.datetime.now()
        print(f"Time taken for all: {datetime.datetime.now() - T}")


        
    T2 = datetime.datetime.now()
    print(f"Time taken: {T2 - T1}")
    


def get_distance(source, dest):
    url = f"https://api.distancematrix.ai/maps/api/distancematrix/json?origins={source}&destinations={dest}&departure_time=now&key=LeYdgyGVVaSPgdAD1KX62vowqKy81"

    response = response = requests.request("GET", url)

    if response.status_code == 200:
        response = response.json()
        if response["rows"][0]["elements"][0]["status"] == "OK":
            distance = response["rows"][0]["elements"][0]["distance"]["text"]
            return distance
        else:
            print(response)
            return "NA"
    else:
        frappe.throw(f"Error in getting distance matrix, status code: {response.text}")
        print(f"Error: {response.text}")


get_distance_matrix(source_cordinates, dest_cordinates)
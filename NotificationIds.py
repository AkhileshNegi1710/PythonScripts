from concurrent.futures import ThreadPoolExecutor, wait
import concurrent
import re
import requests
import csv
import json
import configparser
import pandas as pd

#{{url}}/v2.0/users?pilotId={{pilotId}}
#{{url}}/meta/users/{{uuid}}/homes/1
#{{url}}/zipcode/{{country}}/{{zipcode}}

BaseURL = ""
country = ""
pilotId = ""
ACCESS_TOKEN = ""

UUIDS=[]

user_columns = ["uuid", "NotificationID", "status"]
output = []

def write_to_csv(header, data, month):
    file_path = '/Users/akhileshnegi/Documents/Work/PythonScript/API/MonthlyTracker_HTML/Notification.csv'
    with open(file_path, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)


def updatelist(notID):
    user_api = BaseURL + "/2.1/utility_notifications/users/"+uuid
    user_response = requests.get(url=user_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
    response_JSON=user_response.json()


    for i in range (0,3):
        if(response_JSON["payload"]["notificationsList"][i]["notificationType"] == "EV_MONTHLY_TRACKER"):
            User_id=response_JSON["payload"]["notificationsList"][i]["userId"]
            NotificationID=response_JSON["payload"]["notificationsList"][i]["notificationId"]
            NotificationType=response_JSON["payload"]["notificationsList"][i]["notificationType"]
            LastMonth_Cost=response_JSON["payload"]["notificationsList"][i]["notificationTitle"]
            GenerationTimestamp=response_JSON["payload"]["notificationsList"][i]["generationTimestamp"]
            status=response_JSON["payload"]["notificationsList"][i]["status"]
            #        "EventOPT_OUT": Save_OPT_OUT}
            print(notID,"|",NotificationID,"|",status)
        
        else:
            print("User not found ",notID)    
    output.append([notID,NotificationID,status])


executor = ThreadPoolExecutor(max_workers=100)
result = []
for j in UUIDS:
    result.append(executor.submit(updatelist,j))
done, not_done = wait(result, return_when=concurrent.futures.ALL_COMPLETED)
print("Number of completed tasks: ", len(done), " and incomplete tasks: ", len(not_done))
write_to_csv(user_columns,output,"TOUCluster")


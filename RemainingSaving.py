from concurrent.futures import ThreadPoolExecutor, wait
import concurrent
import re
import requests
import csv
import json
import configparser
import pandas as pd



BaseURL = "url"
country = ""
pilotId = ""
ACCESS_TOKEN = ""

NotificationID=[]

user_columns = ["notID", "account_id", "negative", "positive","cost","Month_Email"]
output = []

def write_to_csv(header, data, month):
    file_path = '/Users/akhileshnegi/Documents/Work/PythonScript/API/MonthlyTracker_HTML/' + month + ".csv"
    with open(file_path, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)

def updatelist(notID):
	user_api = BaseURL + "v3.0/notifications/"+notID+"/body"
	user_response = requests.get(url=user_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	response_JSON=user_response.json()
	account_id=response_JSON["payload"]["externalUserId"]

	
	# for negative difference
	# size:', '28px;line-height:', '42px;">', '<p', 'style="margin:0;">-75%</p>', '</td>''
	negative = re.compile(r'([0-9]+)([a-z]+)\;([a-z]+)\-([a-z]+)\:\'\,\s\'([0-9]+)([a-z]+)\;\"\>\'\,\s\'\<([a-z])\'\,\s\'([a-z]+)\=\"([a-z]+)\:([0-9])\;\"\>\-([0-9]+)\%\</([a-z])\>')
	neg_diff=response_JSON["payload"]["notificationData"]["notificationBody"].strip().replace("\n","").replace("\t","").split()
	Negative_Json=negative.findall(str(neg_diff))
	value=str(Negative_Json)
	# print(value)
	NegativeFinalValue=value[74:76]
	
	# print(NegativeFinalValue)

	# for positive difference
	positive = re.compile(r'\'\<([a-z])\'\,\s\'([a-z]+)\=\"([a-z]+)\:\'\,\s\'([0-9]+)\"\>\+([0-9]+)\%\<\/([a-z]+)\>')

	post_diff=response_JSON["payload"]["notificationData"]["notificationBody"].strip().replace("\n","").replace("\t","")
	
	Positive_Json=positive.findall(str(post_diff))
	print(Positive_Json)

	
	# last and previous month cost
	Cost = re.compile(r'\;([a-z]+)\-([a-z]+)\:([0-9]+)\;\"\>\'\,\s\'\$([0-9]+)')
	Previous_cost=response_JSON["payload"]["notificationData"]["notificationBody"].strip().replace("\n","").replace("\t","").split()
	# print(Previous_cost)
	Cost_Json=Cost.findall(str(Previous_cost))
	lastMonthCost=str(Cost_Json)[28:30]
	# print(lastMonthCost)
	PreviousMonthCost=str(Cost_Json)[61:63]
	# print(PreviousMonthCost)
	
	output.append([notID,account_id,NegativeFinalValue,Positive_Json,lastMonthCost,PreviousMonthCost])
	# print(notID,account_id,NegativeFinalValue,Positive_Json,lastMonthCost,PreviousMonthCost)
        


executor = ThreadPoolExecutor()
result = []
for i in NotificationID:
	result.append(executor.submit(updatelist,i))
done, not_done = wait(result, return_when=concurrent.futures.ALL_COMPLETED)
print("Number of completed tasks: ", len(done), " and incomplete tasks: ", len(not_done))
write_to_csv(user_columns,output,"Outputsaving")

from concurrent.futures import ThreadPoolExecutor, wait
import concurrent
import requests
import csv
import json
import boto3
import os
import pandas as pd
import traceback
from datetime import datetime, timedelta

BaseURL = "url"
country = "US"
pilotId = ""
ACCESS_TOKEN = ""

StartTimestamp="1680321600"
EndStartTimestamp="1682913599"

UserIDs=[]

offPeakRate=0.12583
user_columns=["UserID","RatePlanId","Email","OnPeakConsumption","OnPeakCost","MidPeakConsumption","MidPeakCost","OffPeakConsumption","OffPeakCost","TotalCostMidOnPeak","TotalCostAfterRound","TotalConsumptionMidOnPeak","Saving","SavingAfterRoundOff","Percentage","RoundOffPercentage","StartTimeStamp","EndTimeStamp","DateStartTimestamp"]
output=[]



def write_to_csv(header,data,month):
	file_path = '/Users/akhileshnegi/Documents/Work/PythonScript/API/ChargeAlert/TouChargeAlert1.csv'
	with open(file_path,'w',encoding='UTF8', newline='') as f:
		writer=csv.writer(f)
		writer.writerow(header)
		writer.writerows(data)


def touAggregated(UserID):
	
	# To get PlanNumber
	get_planNumber_api = BaseURL+"/meta/users/"+UserID+"/homes/1"
	planNumber_response_api = requests.get(url=get_planNumber_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	plannumber_response=planNumber_response_api.json()
	planNumber=plannumber_response["plannumber"]
	ratePlanId=plannumber_response["ratePlanId"]
	print(UserID,"--->",planNumber)
	
	# To get Email
	get_email_api = BaseURL+"/meta/users/"+UserID
	email_response_api = requests.get(url=get_email_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	email_response=email_response_api.json()
	Email=email_response["email"]

	# TOU Aggregated API
	get_Tou_Aggregated_API = BaseURL+"/billingdata/users/"+UserID+"/homes/1/aggregatedCost/18/tou?planNumber="+planNumber+"&t0="+StartTimestamp+"&t1="+EndStartTimestamp+"&mode=day&tz=America/New_York"
	tou_aggregated_response_api = requests.get(url=get_Tou_Aggregated_API, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	tou_aggregated_response = tou_aggregated_response_api.json()

	count=0
	getTouRcrMap=list(tou_aggregated_response)

	try:
		for i in getTouRcrMap:
		
			touRcrMap=tou_aggregated_response[i]["touAggData"]["touRrcMap"]
			
			# checking if data is available or not
			if not touRcrMap:
				count += 1
			
			if len(touRcrMap)!=0:
				print("touRcrMap is present for this timestamp ",i,UserID)
				
				# On Peak 

				if "On-Peak" in touRcrMap:
					print("On-Peak is Present",i,UserID)
					OnPeak=touRcrMap["On-Peak"]
					OnPeakConsumption=OnPeak["tierCons"]
					OnPeakCost=OnPeak["tierCost"]
					OnPeakRate=OnPeak["max"]
					# print("On COst-->",OnPeakCost," ",OnPeakConsumption, i)
					
				else:
					print("No On-Peak is present ",i,UserID)
					OnPeakConsumption=0.0
					OnPeakCost=0.0

				# Mid Peak
				if "Mid-Peak" in touRcrMap:
					print("Mid Peak is Present ",i,UserID)
					MidPeak=touRcrMap["Mid-Peak"]
					MidPeakConsumption=MidPeak["tierCons"]
					MidPeakCost=MidPeak["tierCost"]
					MidPeakRate=MidPeak["max"]
					# print("Mid Peak-->",MidPeakCost," ", MidPeakConsumption)
					
				else:
					print("No Mid Peak is peak ",i,UserID)
					MidPeakConsumption=0.0
					MidPeakCost=0.0

				# Off Peak
				if "Off-Peak" in touRcrMap:
					print("Off Peak is present",i,UserID)
					OffPeak=touRcrMap["Off-Peak"]
					OffPeakConsumption=OffPeak["tierCons"]
					OffPeakCost=OffPeak["tierCost"]
					OffPeakRate=OffPeak["min"]
					# print("Off COST---> ",OffPeakCost," ", OffPeakConsumption)
					
				else:
					print("No Off Peak ",i,UserID)
					OffPeakConsumption=0.0
					OffPeakCost=0.0


				# Total Cost On Peak + Mid Peak
				TotalCostMidOnPeak = OnPeakCost + MidPeakCost

				# Total Consumption On Peak + Mid Peak
				TotalConsumptionMidOnPeak = OnPeakConsumption + MidPeakConsumption
				
				# Saving of On-Peak and Mid-Peak
				SavingWithOnPeakMidPeak = TotalCostMidOnPeak - ((TotalConsumptionMidOnPeak/1000) * offPeakRate)

				# Round Saving, TotalCost and Percentage
				Saving=round(SavingWithOnPeakMidPeak)
				TotalCost=round(TotalCostMidOnPeak)
				
				# Calculate %age of On-Peak and Mid-Peak if less than < $4
				
				if(TotalCostMidOnPeak > 0):
					CalculatePercentage=(SavingWithOnPeakMidPeak / TotalCostMidOnPeak) * 100
				else:
					CalculatePercentage=0
				
				Percentage=round(CalculatePercentage)

				datetime_object = datetime.fromtimestamp(int(i))
				# increasing date 24 hours
				endDate_object=datetime.fromtimestamp(int(i))+ timedelta(hours=24)
				# converting to timetstamp
				endDatetimestamp = datetime.timestamp(endDate_object)
				CombinedStartTimestamp=str(datetime_object).split(" ")
				CombinedEndTimestamp=str(endDatetimestamp).split(".")
				DateStartTimestamp=CombinedStartTimestamp[0]
				DateEndTimeStamp=CombinedEndTimestamp[0]
				
				output.append([UserID,ratePlanId,Email,OnPeakConsumption,OnPeakCost,MidPeakConsumption,MidPeakCost,OffPeakConsumption,OffPeakCost,TotalCostMidOnPeak,TotalCost,TotalConsumptionMidOnPeak,SavingWithOnPeakMidPeak,Saving,CalculatePercentage,Percentage,i,DateEndTimeStamp,DateStartTimestamp])
		
		if(count==30 or count==31):
			print("Data is not available for %s days for this user--> %s " % (count, UserID))

	except:
		print("Exception occured for this %s user --> planNumber %s " % (UserID, planNumber))

	


executor = ThreadPoolExecutor()
result = []
for i in UserIDs:
	result.append(executor.submit(touAggregated,i))
done, not_done = wait(result, return_when=concurrent.futures.ALL_COMPLETED)
print("Number of completed tasks: ", len(done), " and incomplete tasks: ", len(not_done))
write_to_csv(user_columns,output,"TOU")

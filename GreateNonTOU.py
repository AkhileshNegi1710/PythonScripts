import threading
from concurrent.futures import ThreadPoolExecutor, wait
import concurrent
import requests
import csv
import json
import pandas as pd
from datetime import datetime, timedelta

BaseURL = ""
country = "US"
pilotId = ""
ACCESS_TOKEN = ""

StartMonthTimestamp="1680321600"
EndMonthTimestamp="1682913599"
offPeakRate=0.1258

UserIDs=[]


user_columns = ["UserID","ratePlanId","Email","Tier_0_Consumption","Tier_0_Cost","Tier_1_Consumption","Tier_1_Cost","TotalCost","RoundTotalCost","TotalConsumption","DecimalSaving","AfterRoundSaving","StartTimestamp","EndTimestamp","DateStartTimestamp"]
output = []

def write_to_csv(header, data, month):
    file_path = '/Users/akhileshnegi/Documents/Work/PythonScript/API/ChargeAlert/NonTouChargeAlert.csv'
    with open(file_path, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)


def touAggregated(UserID):
	
	# To get PlanNumber
	
	get_planNumber_api = BaseURL+"/meta/users/"+UserID+"/homes/1"
	planNumber_response_api = requests.get(url=get_planNumber_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	plannumber_response=planNumber_response_api.json()
	planNumber=plannumber_response["plannumber"]
	ratePlanId=plannumber_response["ratePlanId"]
	# print(planNumber," ", ratePlanId)


	# To get Email
	
	get_email_api = BaseURL+"/meta/users/"+UserID
	email_response_api = requests.get(url=get_email_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	email_response=email_response_api.json()
	Email=email_response["email"]
	# print(Email)


	# TOU Aggregated API
	
	get_Tier_Aggregated_API=BaseURL+"/billingdata/users/"+UserID+"/homes/1/aggregatedCost/18/tier?planNumber="+planNumber+"&t0="+StartMonthTimestamp+"&t1="+EndMonthTimestamp+"&mode=day&tz=America/New_York"
	tier_aggregated_response_api = requests.get(url=get_Tier_Aggregated_API, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	tier_aggregated_response=tier_aggregated_response_api.json()

	# checking exception
	# status_code=tier_aggregated_response["error"]["code"]
	# if status_code == "500":
	# 	print("%s Exception occured for this user %s and please check the plannumber %s" % (status_code, UserID, planNumber))
	count=0
	getTierRcrMap=list(tier_aggregated_response)
	try:
		for i in getTierRcrMap:
			# print("timestamp ",i)
			tierRcrMap=tier_aggregated_response[i]["tierAggData"]["tierRrcMap"]
			
			# checking if data is available or not
			if not tierRcrMap:
					count += 1

			if len(tierRcrMap)!=0:
				# print("tierRcrMap is present for this timestamp ",i,UserID)

				# Checking 0 and 1 Tier is present or not
				if "0" in tierRcrMap:
					zeroTier=tierRcrMap["0"]
					zeroConsumption=zeroTier["tierCons"]
					zeroCost=zeroTier["tierCost"]
					print(zeroConsumption,zeroCost,i)
				
				else:
					print("0 Tier not present",i)
					zeroConsumption=0.0
					zeroCost=0.0

				
				if "1" in tierRcrMap:
					oneTier=tierRcrMap["1"]
					oneConsumption=oneTier["tierCons"]
					oneCost=oneTier["tierCost"]
					print(oneConsumption,oneCost,i)
				
				else:
					print("1 Tier not present",i)
					oneConsumption=0.0
					oneCost=0.0
				
				# Total Cost			
				TotalCost=zeroCost+oneCost

				# Total Consumption
				TotalConsumption=zeroConsumption+oneConsumption

				# Calculate saving
				saving=TotalCost-((TotalConsumption/1000)*offPeakRate)
				# print(saving)
				datetime_object = datetime.fromtimestamp(int(i))
				# print(datetime_object)

				# increasing date 24 hours
				endDate_object=datetime.fromtimestamp(int(i))+ timedelta(hours=24)
				# converting to timetstamp
				endDatetimestamp = datetime.timestamp(endDate_object)
				# print("endDatetimestamp ",endDatetimestamp)

				CombinedStartTimestamp=str(datetime_object).split(" ")
				CombinedEndTimestamp=str(endDatetimestamp).split(".")
				DateStartTimestamp=CombinedStartTimestamp[0]
				DateEndTimeStamp=CombinedEndTimestamp[0]

				# round Saving and Total Cost
				roundSaving=round(saving)
				roundTotalCost=round(TotalCost)

				if(roundSaving >=4):
				# print("reached", UserID)
					output.append([UserID,ratePlanId,Email,zeroConsumption,zeroCost,oneConsumption,oneCost,TotalCost,roundTotalCost,TotalConsumption,saving,roundSaving,i,DateEndTimeStamp,DateStartTimestamp])

		if(count==30 or count==31):
			print("Data is not available for %s days for this user--> %s " % (count, UserID))
	except:
		print("Exception occured for this %s and planumber is %s " % (UserID, PlanNumber))
		
result = []
# threads = []

# def threadfunc(UserId):
# 		thread=threading.Thread(target=touAggregated(UserId),args=UserId,)
# 		thread.start()
# 		threads.append(thread)

# 		for thread in threads:
# 			thread.join()


executor = ThreadPoolExecutor()


# for i in UserIDs:
# 	result.append(executor.submit(touAggregated,i))



try:
	futuresResult= [result.append(executor.submit(touAggregated,i)) for i in UserIDs]
except:
	print("Exception occured")
	executor.shutdown(wait=False, cancel_futures=True)

done, not_done = wait(result, return_when=concurrent.futures.ALL_COMPLETED)
print("Number of completed tasks: ", len(done), " and incomplete tasks: ", len(not_done))
write_to_csv(user_columns,output,"NonTOU")

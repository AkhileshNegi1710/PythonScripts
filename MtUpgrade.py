

from concurrent.futures import ThreadPoolExecutor,wait
from concurrent.futures import ProcessPoolExecutor
import concurrent
import requests
import csv
import json

BaseURL = ""
pilotId = ""
ACCESS_TOKEN = ""

# ---------------------------------------------

# August
CurrentMonthStartTimestamp="1690862400"
CurrentMonthEndTimestamp="1693540799"

# July
PreviousMonthStartTimestamp="1688184000"
PreviousMonthEndTimestamp="1690862399"

# ---------------------------------------------

# startTime=[CurrentMonthStartTimestamp,PreviousMonthStartTimestamp]
# endTime= [CurrentMonthEndTimestamp,PreviousMonthEndTimestamp]
# zipMappedTwoList=zip(startTime, endTime)

TOU_Program_id="43d9fa80-da8e-44c3-b08b-32e1013a5272"
NonTOU_Program_id="d433eb2b-99ef-4a08-861e-2f0cab44758e"


# ---------------------------------------------

# file path
file_path="/Users/akhileshnegi/Documents/Work/PythonScript/upgradeReport/user.txt"

# csv header
header=["UserId","StartTimestamp","EndTimeStamp","MonthlyEvCostGreaterThanThreshold","MonthlyEvConsumptionGreaterThanThreshold","MonthlyConsumptionRatioGreaterThanThreshold","MonthlyThresholdMetForEvData"]

output = []

# ---------------------------------------------
# Thresholds 

ev_monthly_cost_threshold=300
ev_monthly_consumption_kwh_threshold=1500
ev_monthly_consumption_ratio_threshold=0.6
monthly_threshold_for_ev_monthly_tracker_email=90

# ---------------------------------------------
# PlanNumbers

TouPlanNumber=["37","41","42","34"]
NonTouPlanNumber=["1","10"]




# ---------------------------------------------

def updateReport(UserID,start,end):
	# for start, end in zipMappedTwoList:
		EVcost,EVconsumption = homeAPI(UserID,start,end)
		MonthlyConsumption = energyConsumptionAPI(UserID,start,end)
		MonthlyEvCostGreaterThanThreshold = isMonthlyEvCostGreaterThanThreshold(UserID,EVcost,start,end)
		MonthlyEvConsumptionGreaterThanThreshold = isMonthlyEvConsumptionGreaterThanThreshold(UserID,EVconsumption,start,end)
		MonthlyConsumptionRatioGreaterThanThreshold = isMonthlyConsumptionRatioGreaterThanThreshold(UserID,EVconsumption,MonthlyConsumption,start,end)
		MonthlyThresholdMetForEvData = isMonthlyThresholdMetForEvData(UserID,start,end)
		output.append([UserID,start,end,MonthlyEvCostGreaterThanThreshold,MonthlyEvConsumptionGreaterThanThreshold,MonthlyConsumptionRatioGreaterThanThreshold,MonthlyThresholdMetForEvData])
		return output

def isMonthlyThresholdMetForEvData(UserID,start,end):
	billing_cost_api = BaseURL +"/billingdata/users/"+UserID+"/homes/1//consumption/ctypes/billing_cost?t0="+start+"&t1="+end+"&mode=day&tz=America/New_York"
	billingcost_api_response = requests.get(url=billing_cost_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	billingcost_response_JSON = billingcost_api_response.json()
	billingcost = list(billingcost_response_JSON)
	costCount=0
	totalday=0
	
	try:
		for i in billingcost:
			cost=billingcost_response_JSON[i]["cost"]
			if cost!=0.0:
				costCount = costCount + 1
			totalday = totalday + 1
		
		MonthlyCost = (costCount*100)/totalday
		if (MonthlyCost <= monthly_threshold_for_ev_monthly_tracker_email):
			print("CostCount %s, Totalday = %s, MonthlyCost = %s, monthly_threshold_for_ev_monthly_tracker_email = %s " % (costCount, totalday, MonthlyCost, monthly_threshold_for_ev_monthly_tracker_email))
			print("|--------Threshold:  Monthly threshold is not met for either current month or previous month: [ %s - %s ] for user %s :--------|\n" % (start, end, UserID))
			Threshold = MonthlyCost 

		else:
			print("No monthly_threshold_for_ev_monthly_tracker_email found for : StartTimeStamp %s, EndTimeStamp %s " % (start,end))
			Threshold = 0.0
	except Exception as e: 
		print("Exception occured for this user :",UserID,e)
		Threshold = 0.0
	return Threshold

def isMonthlyConsumptionRatioGreaterThanThreshold(UserID,EVconsumption,EnergyConsumption,start,end):
	DivideConsumption = EVconsumption/EnergyConsumption
	if(DivideConsumption >= ev_monthly_consumption_ratio_threshold):
		print("DivideConsumption %s, EVconsumption = %s, EnergyConsumption %s,  ev_monthly_consumption_ratio_threshold = %s " % (DivideConsumption, EVconsumption, EnergyConsumption, ev_monthly_consumption_ratio_threshold))
		print("|--------Threshold:  Consumption ratio is not within threshold for either current month or previous month: [ %s - %s ] for user %s :--------|\n" % (start, end, UserID))
		Threshold = DivideConsumption
	else:
		print("No ev_monthly_consumption_ratio_threshold found for : StartTimeStamp %s, EndTimeStamp %s " % (start,end))
		Threshold = 0.0

	return Threshold
	

def isMonthlyEvConsumptionGreaterThanThreshold(UserID,EVconsumption,start,end):
	if EVconsumption >= ev_monthly_consumption_kwh_threshold:
		print("EVconsumption = %s, ev_monthly_consumption_kwh_threshold = %s " % (EVconsumption, ev_monthly_consumption_kwh_threshold))
		print("|--------Threshold: Ev consumption is greater than threshold for either current month or previous month: [%s - %s] for user %s :--------|\n" % (start,end,UserID))
		Threshold = EVconsumption
	else:
		print("No ev_monthly_consumption_kwh_threshold found for : StartTimeStamp %s, EndTimeStamp %s " % (start,end))
		Threshold = 0.0
	return Threshold



def isMonthlyEvCostGreaterThanThreshold(UserID,EVcost,start,end):
	if(EVcost > ev_monthly_cost_threshold):
		print("EVcost = %s, ev_monthly_cost_threshold = %s " % (EVcost, ev_monthly_cost_threshold))
		print("|--------Threshold: Ev monthly cost is greater than threshold for either current month or previous month: [%s - %s] for user %s :--------|\n" % (start,end,UserID))
		Threshold = EVcost
	else:
		print("No isMonthlyEvCostGreaterThanThreshold found for : StartTimeStamp %s, EndTimeStamp %s " % (start,end))
		Threshold = 0.0
	return Threshold


def energyConsumptionAPI(UserID,start,end):
	energy_consumption_api = BaseURL +"/billingdata/users/"+UserID+"/homes/1/consumption/ctypes/ENERGY_CONSUMPTION?t0="+start+"&t1="+end+"&mode=day"
	energy_consumption_api_response = requests.get(url=energy_consumption_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	energy_consumption_response_JSON = energy_consumption_api_response.json()
	energyConsumption = list(energy_consumption_response_JSON)
	TotalConsumption=0.0
	for i in energyConsumption:
			consumption = energy_consumption_response_JSON[i]["value"]
			TotalConsumption = TotalConsumption + consumption
	TotalMonthlyconsumption = round(TotalConsumption/1000)
	print("\n|------------TotalMonthlyConsumption = %s, UserID = %s , StartTimeStamp = %s, EndTimeStamp = %s ------------|" % (TotalMonthlyconsumption,UserID,start,end))
	return TotalMonthlyconsumption	



def homeAPI(UserID,start,end):
		start,end = int(start), int(end)
		home_api = BaseURL +"/meta/users/"+UserID+"/homes/1"
		home_response = requests.get(url=home_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
		home_response_JSON = home_response.json()
		rateSchedule = home_response_JSON["ratesSchedule"]
		if rateSchedule!="null":
			RateSch_loadJson = json.loads(rateSchedule)
			for i in RateSch_loadJson:
				startTime = i["startTime"]
				endTime = i["endTime"]
				# 15july <= 1 august and 31 august  <= future timestamp(2038)
				if startTime <= start and end <= endTime:
					planNumber = i["metaData"]["planNumber"]
					if planNumber in TouPlanNumber:
						EVcost,EVconsumption = touData(UserID,start,end,planNumber)
					elif planNumber in NonTouPlanNumber:
						EVcost,EVconsumption = nonTOUData(UserID,start,end,planNumber)
					else:
						print("\n |---------Current PlanNumber = %s is not matching with EV planNumbers for this UserID = %s-------------|" % (planNumber, UserID))
				# 1 August <= 18 August <= 31 August
				elif start <= endTime <= end:
					print("|---------- Rate Transition User = %s -------|" % (UserID))
					EVconsumption,EVcost = 0.0,0.0
					planNumber1 = i["metaData"]["planNumber"]
					nextElement = RateSch_loadJson.index(i) + 1
					planNumber2 = RateSch_loadJson[nextElement]["metaData"]["planNumber"]
					if planNumber1 in TouPlanNumber:
						EVcost1,EVconsumption1 = touData(UserID,start,endTime,planNumber1)
					elif planNumber1 in NonTouPlanNumber:
						EVcost1,EVconsumption1 = nonTOUData(UserID,start,endTime,planNumber1)
					else:
						print("\n |---------Current PlanNumber = %s is not matching with EV planNumbers for this UserID = %s-------------|" % (planNumber1, UserID))
					if planNumber2 in TouPlanNumber:
						EVcost2,EVconsumption2 = touData(UserID,endTime,end,planNumber2)
					elif planNumber2 in NonTouPlanNumber:
						EVcost2,EVconsumption2 = nonTOUData(UserID,endTime,end,planNumber2)
					else:
						print("\n |---------Current PlanNumber = %s is not matching with EV planNumbers for this UserID = %s-------------|" % (planNumber2, UserID))	
					EVcost = EVcost1 + EVcost2
					EVconsumption = EVconsumption1 + EVconsumption2
					print("Total Cost %s  and Consumption %s for this User %s start - end [%s %s] " % (EVcost,EVconsumption,UserID,start,end))

		else:
			print("RateSchedule is null for this user: ",UserID)
			planNumber = home_response_JSON["plannumber"]
			if planNumber in TouPlanNumber:
				EVcost,EVconsumption = touData(UserID,start,end,planNumber)

			elif planNumber in NonTouPlanNumber:
				EVcost,EVconsumption = nonTOUData(UserID,start,end,planNumber)
			else:
				print("\n |---------Current PlanNumber = %s is not matching with EV planNumbers for this UserID = %s-------------|" % (planNumber, UserID))	
		return EVcost,EVconsumption


def touData(UserID,start,end,planNumber):
			start,end = str(start), str(end)
			print("\n|------------ Checking for TOU User = %s, PlanNumber = %s , StartTimeStamp = %s, EndTimeStamp = %s --------|\n" % (UserID, planNumber,start,end))
			current_aggregated_api = BaseURL + "/billingdata/users/"+UserID+"/homes/1/aggregatedCost/18/tou?planNumber="+planNumber+"&t0="+start+"&t1="+end+"&mode=month&tz=America/New_York"
			current_aggregated_response = requests.get(url=current_aggregated_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
			current_aggregated_response_JSON = current_aggregated_response.json()
			gettouRrcMap=list(current_aggregated_response_JSON)
			try:
				CurrentTotalCostPeak,CurrentTotalConsumptionPeak = 0.0,0.0
				for i in gettouRrcMap:
					touRrcMap=current_aggregated_response_JSON[i]["touAggData"]["touRrcMap"]
					if len(touRrcMap)==0:
						CurrentTotalCostPeak,CurrentTotalConsumptionPeak = 0.0, 0.0
					if len(touRrcMap)!=0:
						if "On-Peak" in touRrcMap:
							OnPeak=touRrcMap["On-Peak"]
							OnPeakConsumption=OnPeak["tierCons"]
							OnPeakCost=OnPeak["tierCost"]
							OnPeakRate=OnPeak["max"]
						else:
							OnPeakConsumption,OnPeakCost = 0.0, 0.0
						if "Mid-Peak" in touRrcMap:
							MidPeak=touRrcMap["Mid-Peak"]
							MidPeakConsumption=MidPeak["tierCons"]
							MidPeakCost=MidPeak["tierCost"]
							MidPeakRate=MidPeak["max"]
						else:
							MidPeakConsumption,MidPeakCost = 0.0, 0.0
						if "Off-Peak" in touRrcMap:
							OffPeak = touRrcMap["Off-Peak"]
							OffPeakConsumption = OffPeak["tierCons"]
							OffPeakCost = OffPeak["tierCost"]
							OffPeakRate = OffPeak["min"]
						else:
							OffPeakConsumption,OffPeakCost = 0.0, 0.0
						CurrentTotalCostPeak = CurrentTotalCostPeak + OnPeakCost + MidPeakCost + OffPeakCost
						CurrentTotalConsumptionPeak = CurrentTotalConsumptionPeak + OnPeakConsumption + MidPeakConsumption + OffPeakConsumption
				TotalCost=round(CurrentTotalCostPeak)
				TotalConsumption=round(CurrentTotalConsumptionPeak/1000)
			except Exception as e:
				print("Exception occured for this TOU %s user --> PlanNumber %s , startTimeStamp = %s, endTimeStamp = %s" % (UserID, planNumber,start,end))
				print("Exception found: ",e)
			return TotalCost,TotalConsumption

def nonTOUData(UserID,start,end,planNumber):
			start, end = str(start), str(end)
			print("\n|-------- Checking for Non TOU User = %s, PlanNumber = %s,  StartTimeStamp = %s, EndTimeStamp = %s  ----------|\n" % (UserID, planNumber,start,end))
			get_Tier_Aggregated_API=BaseURL+"/billingdata/users/"+UserID+"/homes/1/aggregatedCost/18/tier?planNumber="+planNumber+"&t0="+start+"&t1="+end+"&mode=day&tz=America/New_York"
			tier_aggregated_response_api = requests.get(url=get_Tier_Aggregated_API, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
			tier_aggregated_response = tier_aggregated_response_api.json()
			getTierRcrMap=list(tier_aggregated_response)
			try:
				TotalCostEV, TotalConsumptionEV = 0.0, 0.0
				for i in getTierRcrMap:
					tierRcrMap=tier_aggregated_response[i]["tierAggData"]["tierRrcMap"]
					if len(tierRcrMap) == 0:
						TotalCost,TotalConsumption = 0.0, 0.0
					if len(tierRcrMap)!=0:
						if "0" in tierRcrMap:
							zeroTier = tierRcrMap["0"]
							zeroConsumption = zeroTier["tierCons"]
							zeroCost = zeroTier["tierCost"]
						else:
							zeroConsumption,zeroCost = 0.0, 0.0
						if "1" in tierRcrMap:
							oneTier = tierRcrMap["1"]
							oneConsumption = oneTier["tierCons"]
							oneCost = oneTier["tierCost"]
						else:
							oneConsumption, oneCost = 0.0, 0.0
						TotalCostEV = TotalCostEV+zeroCost+oneCost
						TotalConsumptionEV = TotalConsumptionEV+zeroConsumption+oneConsumption
				TotalCost = round(TotalCostEV)
				TotalConsumption = round(TotalConsumptionEV/1000)
			except Exception as e:
				print("Exception occured for this NonTOU %s user --> PlanNumber %s , startTimeStamp = %s, endTimeStamp = %s" % (UserID,planNumber,start,end))
				print("Exception found :",e)
			return TotalCost,TotalConsumption


def write_to_csv(header, output):
    file_path = 'Threshold_Monthly_Tracker.csv'
    with open(file_path, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(output)




if __name__ == '__main__':
	file1 = open(file_path, 'r')
	Lines = file1.readlines()
	result = []
	count = 0
	executor = ThreadPoolExecutor()
	for line in Lines:
		UserID = line.strip()
		result.append(executor.submit(updateReport,UserID,CurrentMonthStartTimestamp,CurrentMonthEndTimestamp))
		result.append(executor.submit(updateReport,UserID,PreviousMonthStartTimestamp,PreviousMonthEndTimestamp))
		count = count + 1
	done, not_done = wait(result, return_when=concurrent.futures.ALL_COMPLETED)
	print("Number of completed tasks: ", len(done), " and incomplete tasks: ", len(not_done))
	print("count of user executed", count)
	write_to_csv(header, output)
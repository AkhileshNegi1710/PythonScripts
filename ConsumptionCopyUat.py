from concurrent.futures import ThreadPoolExecutor,wait
import concurrent
import requests
import csv
import json
from operator import itemgetter
from datetime import datetime


BaseURL = ""
pilotId = ""

# "************** Please, Add Access Token **************")
ACCESS_TOKEN = ""

# "************** Please, Add the Start Timestamp to get RAW Data **************")
StartMonthTimeStamp="1654041600"

# "************** Please, Add the End Timestamp to get RAW Data **************")
EndMonthTimeStamp="1687947226"

# "************** Please, Add path for storing UserEnrollment and Raw files **************")
IngestionPathFile="/Users/akhileshnegi/Documents/Work/PythonScript/API/RAWCopyConsumptionUAT"


# "************** Please, Add list of uuids **************")

UUIDS=[]








def userEnrollmentFile(UserID):
	
	if(UserID):
		UserEnrollmentFile_path=open(IngestionPathFile+'/USERENROLL_D_'+UserID+'_.txt','w')
		user_api = BaseURL + "/v2.0/users/"+UserID
		user_response = requests.get(url=user_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
		response_JSON=user_response.json()
		Rate_payload=response_JSON["payload"]["homeAccounts"]["rate"]["ratePlanId"]
		Account_number=response_JSON["payload"]["utilityTags"]["account_number"]
		Premise_number_json=response_JSON["payload"]["utilityTags"]["account_and_premise_number"]
		Premise_number=Premise_number_json.split(":")[1]
		Customer_id_create=Premise_number[0:6]
		userDataCombined="tyueHy"+Customer_id_create+"u|t"+Account_number+"|t"+Premise_number+"|bidgelyqa+user+t"+Account_number+"_"+Rate_payload+"@bidgely.com|User_t"+Account_number+"_"+Rate_payload+"|Tracker|221B Baker Street||||BROOKLYN|NY|11691||221B Baker Street||||BROOKLYN|NY|11691||HOME|EN|||ELECTRIC|2015-01-01|2030-01-01|"+Rate_payload+"|2019-07-01|BCC_PSEG_1||FALSE|AMI||\n"
		UserEnrollmentFile_path.writelines(userDataCombined)
		UserEnrollmentFile_path.close()

	


def invoiceFile(UserID):
	
	# solarStatus=False
	if(UserID):
		invoice_path=open(IngestionPathFile+'/INVOICE_'+UserID+'_.txt','w')
		# userEnrollment
		user_enroll_api = BaseURL + "/v2.0/users/"+UserID
		get_response = requests.get(url=user_enroll_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
		response_JSON1=get_response.json()
		Account_number=response_JSON1["payload"]["utilityTags"]["account_number"]
		print(UserID," t"+Account_number)
		Premise_number_json=response_JSON1["payload"]["utilityTags"]["account_and_premise_number"]
		Premise_number=Premise_number_json.split(":")[1]
		Customer_id_create=Premise_number[0:6]
		
		# invoice
		
		user_api = BaseURL + "/billingdata/users/"+UserID+"/homes/1/utilitydata?t0="+StartMonthTimeStamp+"&t1="+EndMonthTimeStamp+"&measurementType=ELECTRIC"
		user_response = requests.get(url=user_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
		response_JSON=user_response.json()
		DayIs=0
		convertolist=list(response_JSON)
		for i in convertolist:
			BillStart=response_JSON[i]["billingStartTs"]
			ConvertBillStart=datetime.fromtimestamp(BillStart)
			ConvertBillStart=str(ConvertBillStart)[0:-11]
			# print(ConvertBillStart)
			BillEnd=response_JSON[i]["billingEndTs"]
			ConvertBillEnd=datetime.fromtimestamp(BillEnd)
			ConvertBillEnd=str(ConvertBillEnd)[0:-11]
			# print(ConvertBillEnd)
			CalendraDay=str(ConvertBillStart)[5:-1]
			# print("check",CalendraDay)
			EvenDays=["04","06","08","10","12"]
			OddDays=["01","03","05","07","09","11"]
			FebDay=["02"]
			if CalendraDay in EvenDays:
				DayIs=30
			if CalendraDay in OddDays:
				DayIs=31
			if(CalendraDay=="02"):
				DayIs=28
			BillConsumption=response_JSON[i]["value"]
			BillCost=response_JSON[i]["cost"]
			# if response_JSON[i]["bidgelyGeneratedInvoice"] == solarStatus:
			# 	SolarValue=response_JSON[i]["invoiceDataList"][0]["metaData"][25:-1]
			# 	print(SolarValue)
			# else:
			# 	SolarValue=True;
			InvoiceDataFile="tyueHy",Customer_id_create,"u|t",Account_number,"|t",Premise_number,"||ELECTRIC|BCC_PSEG_1|",ConvertBillStart,"01|",ConvertBillEnd,"01|",DayIs,"|TOTAL AMOUNT|TOTAL|",BillConsumption,"|",BillCost,"|AMI||"
			OriginalInvoice=str(InvoiceDataFile).replace(", ","").replace("'","").replace("(","").replace(")","")+"\n"
			invoice_path.writelines(OriginalInvoice)
		invoice_path.close()			
			


	
				
		


def rawFile(UserID):
	
	if(UserID):
		RAWFILE_path=open(IngestionPathFile+'/RAW_D_900_'+UserID+'.txt','w')
		user_api = BaseURL + "/streams/users/"+UserID+"/homes/1/gws/2/gb.json?t0="+StartMonthTimeStamp+"&t1="+EndMonthTimeStamp
		user_response = requests.get(url=user_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
		response_JSON=user_response.json();
		FailedCount=0
		# WriteFileRAW=""

		# 2972 is highest data points seen in the RAW API for 31 days
		
		for i in range(0,1000000):
				try:

					result=response_JSON[i]
					cost=result["value"]/1000
					usagePointId=result["usagePointId"]
					Timestamp=result["time"]
					datetime_object = datetime.fromtimestamp(Timestamp)
					CombinedTimestamp=str(datetime_object).replace("-","").replace(":","").replace(" ","")
					StringSplit=usagePointId.split(":")
					PartnerId=StringSplit[1].split("_")[1]
					PremiseID=StringSplit[1].split("_")[2]
					CreateNewId=PremiseID[0:4]+PartnerId[6:9]
					PSEGRAWData="D|a",CreateNewId,"|t",PremiseID,"|0.0.9.4.1.1.12.0.0.0.0.0.0.0.0.3.72.0|3.0.0|",CombinedTimestamp,"|",cost,"|||1|1|t",PartnerId,"|"
					ZeroData="D|a",CreateNewId,"|t",PremiseID,"|0.0.9.4.19.1.12.0.0.0.0.0.0.0.0.3.72.0|3.0.0|",CombinedTimestamp,"|0.0|||1|1|t",PartnerId,"|"
					WriteFileRAW=str(PSEGRAWData).replace(", ","").replace("'","").replace("(","").replace(")","")+"\n"+str(ZeroData).replace(", ","").replace("'","").replace("(","").replace(")","")+"\n"
					RAWFILE_path.writelines(WriteFileRAW)


					# print(RAWFILE_path)
				
				except:
					FailedCount=FailedCount+1
					# print("FAILED-------",FailedCount)
		RAWFILE_path.close()
	



executor = ThreadPoolExecutor()
rawResult = []
userEnrollment=[]
invoice=[]
for i in UUIDS:
	rawResult.append(executor.submit(rawFile,i))
	userEnrollment.append(executor.submit(userEnrollmentFile,i))
	invoice.append(executor.submit(invoiceFile,i))

done, not_done = wait(rawResult, return_when=concurrent.futures.ALL_COMPLETED)
complete, incomplete = wait(userEnrollment, return_when=concurrent.futures.ALL_COMPLETED)
completed, incompleted = wait(invoice, return_when=concurrent.futures.ALL_COMPLETED)

print("RAW file created: ", len(done), " and RAW file not created: ", len(not_done))
print("userEnrollment created: ", len(complete), " and userEnrollment not created: ", len(incomplete))
print("Invoice created: ", len(completed), " and userEnrollment not created: ", len(incompleted))
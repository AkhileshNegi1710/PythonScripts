from concurrent.futures import ThreadPoolExecutor, wait
import concurrent
import requests
import csv
import json



BaseURL = ""
country = ""
pilotId = ""
ACCESS_TOKEN = ""

NotificationID=[]

user_columns = ["uuid","key_data_ingestion","value_data_ingestion","key_disagg_preference","value_disagg_preference","RaisegbUploadKey","RaisegbUploadValue"]
output = []

def write_to_csv(header, data, month):
    file_path = '/Users/akhileshnegi/Documents/Work/PythonScript/API/RaiseGbDisagg/ConfigMTD.csv'
    with open(file_path, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)

def updatelist(notID):
	user_api = BaseURL + "/entities/user/"+notID+"/configs"

	user_response = requests.get(url=user_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN})
	response_JSON=user_response.json()

	uploadEvent=response_JSON["data_ingestion"]
	uploadEvent_OPT_IN=json.loads(uploadEvent)
	key_Uploaddata_ingestion=uploadEvent_OPT_IN["kvs"][0]["key"]

	uploadEventValue=response_JSON["data_ingestion"]
	UploadValue_OPT_IN1=json.loads(uploadEventValue)
	value_UPloaddata_ingestion=UploadValue_OPT_IN1["kvs"][0]["val"]



	OPT_IN=response_JSON["data_ingestion"]
	MT_OPT_IN=json.loads(OPT_IN)
	key_data_ingestion=MT_OPT_IN["kvs"][13]["key"]

	OPT_IN1=response_JSON["data_ingestion"]
	MT_OPT_IN1=json.loads(OPT_IN1)
	value_data_ingestion=MT_OPT_IN1["kvs"][13]["val"]

	OPT_OUT=response_JSON["disagg_preference"]
	MT_OPT_OUT=json.loads(OPT_OUT)
	key_disagg_preference=MT_OPT_OUT["kvs"][3]["key"]


	OPT_OUT1=response_JSON["disagg_preference"]
	MT_OPT_OUT1=json.loads(OPT_OUT1)
	value_disagg_preference=MT_OPT_OUT1["kvs"][3]["val"]


	ActivityMapKey=response_JSON["frontend_configs"]
	ActivityMapJsonKey=json.loads(ActivityMapKey)
	Key_ActivityMap=ActivityMapJsonKey["kvs"][250]["key"]

	ActivityMapValue=response_JSON["frontend_configs"]
	ActivityMapJsonValue=json.loads(ActivityMapValue)
	Value_ActivityMap=ActivityMapJsonValue["kvs"][250]["val"]


	print(notID,"|",key_data_ingestion,"|",value_data_ingestion,"|",key_disagg_preference,"|",value_disagg_preference,"|",key_Uploaddata_ingestion,"|",value_UPloaddata_ingestion,"|",Key_ActivityMap,"|",Value_ActivityMap)

	output.append([notID,key_data_ingestion,value_data_ingestion,key_disagg_preference,value_disagg_preference,key_Uploaddata_ingestion,value_UPloaddata_ingestion])


executor = ThreadPoolExecutor()
result = []
for i in NotificationID:
	result.append(executor.submit(updatelist,i))
done, not_done = wait(result, return_when=concurrent.futures.ALL_COMPLETED)
print("Number of completed tasks: ", len(done), " and incomplete tasks: ", len(not_done))
write_to_csv(user_columns,output,"TOUCluster")

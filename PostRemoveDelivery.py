from concurrent.futures import ThreadPoolExecutor, wait
import concurrent
import requests
import csv
import json



BaseURL = ""
pilotId = ""
ACCESS_TOKEN = ""

UUIDS=[]


def deliveryModes(UserID):
		json_object = [
		{
    "configType": "event_subscriptions.EVENT_NAME.ELECTRIC.OPT_IN",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

    {
    "configType": "event_subscriptions.EVENT_NAME.ELECTRIC",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

    {
    "configType": "event_subscriptions.EVENT_NAME.OPT_OUT",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

       {
    "configType": "event_subscriptions.EVENT_NAME.OPT_IN",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

    {
    "configType": "event_subscriptions.EVENT_NAME",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

    {
    "configType": "event_subscriptions.EVENT_NAME.ELECTRIC.OPT_OUT",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

     {
    "configType": "event_subscriptions.EVENT_NAME.ELECTRIC.DEFAULT",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },
    {
    "configType": "event_subscriptions.EVENT_NAME.ELECTRIC.OPT_IN",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

    {
    "configType": "event_subscriptions.EVENT_NAME.ELECTRIC",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

    {
    "configType": "event_subscriptions.EVENT_NAME.OPT_OUT",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

       {
    "configType": "event_subscriptions.EVENT_NAME.OPT_IN",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

    {
    "configType": "event_subscriptions.EVENT_NAME",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

    {
    "configType": "event_subscriptions.EVENT_NAME.ELECTRIC.OPT_OUT",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    },

     {
    "configType": "event_subscriptions.EVENT_NAME.ELECTRIC.DEFAULT",
    "configKVs": [{
        "configKey": "delivery_modes",
        "configVal": "[]",
        "configDocumentation" : "Event event_subscriptions configs",
        "configRegex" : ".*",
        "configDataType" : "TEXT"
     }]
    }
		]

		for i in json_object:
			# print(i, UserID)
			user_api = BaseURL + "/entities/"+UserID+"/configs"
			user_response = requests.post(url=user_api, params={"pilotId":pilotId,"access_token": ACCESS_TOKEN}, json=i)
			print(UserID,i," --> ",user_response.status_code)


	

executor = ThreadPoolExecutor()
result = []
for i in UUIDS:
	result.append(executor.submit(deliveryModes,i))
done, not_done = wait(result, return_when=concurrent.futures.ALL_COMPLETED)
print("Number of completed tasks: ", len(done), " and incomplete tasks: ", len(not_done))
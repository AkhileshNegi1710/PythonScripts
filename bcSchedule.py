from concurrent.futures import ThreadPoolExecutor, wait
import concurrent
import requests
import csv
import json
from datetime import datetime, timedelta


base_url = ""
pilot_id = ""
access_token = ""
csv_file_name = "bc2.csv"
run_bc_api_check = False


session = requests.Session()



def read_from_csv():
	processed_bc_codes = set()
	count = 0
	if check_config_bc_end(session) == "false":
		print("isEndDateInclusive is False")
		count = 2
		with open(csv_file_name, 'r', encoding='utf-8-sig') as file:
			csv_reader = csv.reader(file, delimiter="|")
			first_row = next(csv_reader, None)
			previous_end_date = None
			if first_row:
				first_bc_code = first_row[0]
				first_start_date = first_row[1]
				first_end_date = first_row[2]
				previous_end_date = first_end_date
				previous_bc_code = first_bc_code
				if first_bc_code not in processed_bc_codes:
					check_bc_code(session,first_bc_code,first_start_date,first_end_date,count)
					processed_bc_codes.add(first_bc_code)


			all_equal = True
			for row in csv_reader:
				bc_code = row[0]
				start_date = row[1]
				end_date = row[2]

				if bc_code not in processed_bc_codes:
					check_bc_code(session,bc_code,start_date,end_date,count)
					processed_bc_codes.add(bc_code)

				if previous_end_date != start_date and previous_bc_code == bc_code:
					all_equal = False
					print("|------- CSV Sheet: For BC code = %s ,Previous End date %s and Start date %s is not Matching -------|\n" % (bc_code,previous_end_date,start_date))

				previous_end_date = end_date
				previous_bc_code = bc_code
			if all_equal:
				print("|------ CSV Sheet: All end and next line start dates are matching ------|\n")
		# print("processed_bc_codes",processed_bc_codes)
	else:
		print("isEndDateInclusive is True")
		count = 3
		with open(csv_file_name, 'r', encoding='utf-8-sig') as file:
			csv_reader = csv.reader(file, delimiter="|")
			first_row = next(csv_reader, None)
			previous_end_date = None
			if first_row:
				first_bc_code = first_row[0]
				first_start_date = first_row[1]
				first_end_date = first_row[2]
				if first_bc_code not in processed_bc_codes:
					check_bc_code(session,first_bc_code,first_start_date,first_end_date,count)
					processed_bc_codes.add(first_bc_code)
				#  current_month = str(int(current_month) + 1).zfill(2)
				previous_end_date = first_end_date
				previous_bc_code = first_bc_code
			all_equal = True
			for row in csv_reader:
				bc_code = row[0]
				start_date = row[1]
				end_date = row[2]

				if bc_code not in processed_bc_codes:
					check_bc_code(session,bc_code,start_date,end_date,count)
					processed_bc_codes.add(bc_code)

				previous_date_obj = datetime.strptime(previous_end_date, '%Y-%m-%d')
				new_previous_date_obj = previous_date_obj + timedelta(days=1)
				new_previous_date_str = new_previous_date_obj.strftime('%Y-%m-%d')
				# prev end date + 1 != next start
				if new_previous_date_str != start_date and previous_bc_code == bc_code:
					all_equal = False
					print("|------- CSV Sheet: For BC code = %s, Previous End date %s and Start date %s should match -------|\n" % (bc_code,previous_end_date,start_date))
				previous_end_date = end_date
				previous_bc_code = bc_code
			if all_equal:
				print("|------ CSV Sheet: All dates support inclusive format ------|\n")
		# print("processed_bc_codes",processed_bc_codes)





def check_bc_code(session,bc_code,start_date,end_date,count):
	user_api = base_url + "/2.1/utilityBillingCycles/utility/"+pilot_id+"/identifier/"+bc_code
	user_response = session.get(url=user_api, params={"pilotId":pilot_id,"access_token": access_token})
	response_JSON = user_response.json()
	payload_response = response_JSON["payload"]
	if payload_response:
		last_billing_cycle_entry = payload_response[-1]
		end_timestamp = last_billing_cycle_entry["validTo"]
		# start_timestamp_validFrom = datetime.fromtimestamp(start_timestamp)
		# start_date_string = start_timestamp_validFrom.strftime("%Y-%m-%d")
		
		end_timestamp_validTo = datetime.fromtimestamp(end_timestamp)
		end_date_string = end_timestamp_validTo.strftime("%Y-%m-%d")

		end_timestamp_minus_two_day = end_timestamp_validTo 
		end_date_string_two = end_timestamp_minus_two_day.strftime("%Y-%m-%d")



		
		if count == 2 and end_date_string != start_date:
			print("|------- BC Schedule API:  End date of BC code API = %s is not matching with Start date = %s of BC code = %s  -------|\n" % (end_date_string,start_date,bc_code))

		if count == 3 and end_date_string_two != start_date:
			print("|------- BC Schedule API:  End date of BC code API = %s is not matching with Start date = %s of BC code = %s  -------|\n" % (end_date_string_two,start_date,bc_code))

	else:
		print("No BC code = %s found in BC Schedule API" % (bc_code))
	return count


def check_config_bc_end(session):
	user_api = base_url + "/entities/pilot/"+pilot_id+"/configs/"
	user_response = session.get(url=user_api, params={"pilotId":pilot_id,"access_token": access_token})
	response_JSON = user_response.json()
	email_template = response_JSON["launchpad_ingestion_configs"]
	email_template_json =json.loads(email_template)
	key_email_template_json = email_template_json["kvs"]
	for i in range(0,len(key_email_template_json)):
		key_email_json = key_email_template_json[i]["key"]
		if(key_email_json == "isEndDateInclusive"):
			value_billing_end_date = key_email_template_json[i]["val"]
			break
	return value_billing_end_date	


def fetch_bc_code():
	unique_bc_codes = set()
	count = 2
	# step of 100
	
	for offset in range(0,3001,100):
		user_api = base_url + "/2.1/utilityBillingCycles/utility/"+pilot_id
		user_response = session.get(url=user_api, params={"pilotId":pilot_id,"access_token": access_token,"offset":offset})
		response_JSON = user_response.json()
		bc_code_json = response_JSON["payload"]["data"]
		for i in range(0,len(bc_code_json)):
			total_bc_code = bc_code_json[i]["billCycleCode"]
			unique_bc_codes.add(total_bc_code)
	# print(unique_bc_codes)
	for bc_code in unique_bc_codes:
		bc_schedule_api = base_url + "/2.1/utilityBillingCycles/utility/"+pilot_id+"/identifier/"+bc_code
		bc_schedule_json = session.get(url=bc_schedule_api, params={"pilotId":pilot_id,"access_token": access_token})
		response_JSON = bc_schedule_json.json()
		payload_response = response_JSON.get("payload",[])
		count = 0
		for billing_cycle_code in range(len(payload_response)-1):
			# start_time = payload_response[billing_cycle_code]["validFrom"]
			end_time = payload_response[billing_cycle_code]["validTo"]
			billCycleCode = payload_response[billing_cycle_code]["billCycleCode"]
			next_start_time = payload_response[billing_cycle_code + 1]["validFrom"]
			# next_end_time = payload_response[billing_cycle_code + 1]["validTo"]
			if(end_time != next_start_time):
				print("|------ Date Error in BC API: For BC Code %s, End Data %s is matching with %s next Start date -----|\n" % (billCycleCode,end_time,next_start_time))
			else:
				count  = count + 1
			# print(start_time,next_start_time)
		# if (count == len(payload_response) -1):
		# 	print("|------ No Mismatch for dates in BC API for this BC code = %s -----|\n" % (bc_code))





executor = ThreadPoolExecutor()
result = []
bc_api_output = []

if run_bc_api_check:
	result.append(executor.submit(fetch_bc_code))
bc_api_output.append(executor.submit(read_from_csv))

done, not_done = wait(result, return_when=concurrent.futures.ALL_COMPLETED)
print("Number of completed tasks: ", len(done), " and incomplete tasks: ", len(not_done))

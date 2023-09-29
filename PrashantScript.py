import concurrent
import csv
import json
from concurrent.futures import ThreadPoolExecutor, wait

import pandas as pd
import requests

data = []
count = 0
output_file_path = ""
access_token = ""
url = ""
tou_program_id = "43d9fa80-da8e-44c3-b08b-32e1013a5272"
non_tou_program_id = "d433eb2b-99ef-4a08-861e-2f0cab44758e"

def get_plan_number(uuid):
    URL = "/meta/users/%s/homes/1/" % uuid
    PARAMS = {"access_token": access_token}

    r = requests.get(url=URL, params=PARAMS)
    data = r.json()

    if data["plannumber"] in ['37', '34', '41', '42']:
        print("uuid: ", uuid, " Plan Number: ", data["plannumber"])
    return data["plannumber"]


def check_overlapping_rates(rate_schedule):
    if len(rate_schedule) <= 1:
        return False

    previous_rate = rate_schedule[0]
    for i in range(1, len(rate_schedule)):
        current_rate = rate_schedule[i]
        if current_rate["startTime"] < previous_rate["endTime"]:
            return True
        previous_rate = current_rate
    return False


def extract_time(json):

    try:
        return int(json['startTime'])
    except KeyError:
        return 0


def get_rate_plan_schedule(uuid, dates):
    URL = "%s/meta/users/%s/homes/1/" % (url, uuid)
    PARAMS = {"access_token": access_token}
    r = requests.get(url=URL, params=PARAMS)
    data = r.json()

    result = []

    if not data["ratesSchedule"]:
        updated_rate_schedule = dict()
        updated_rate_schedule["startTime"] = dates[0]
        updated_rate_schedule["endTime"] = dates[1]
        planNumber = data["plannumber"]
        if planNumber in ['37', '34', '41', '42']:
            updated_rate_schedule["programId"] = tou_program_id
        else:
            updated_rate_schedule["programId"] = non_tou_program_id
        updated_rate_schedule["planNumber"] = planNumber
        result.append(updated_rate_schedule)
        return result

    rate_schedules = json.loads(data["ratesSchedule"])
    rate_schedules.sort(key=extract_time, reverse=False)
    if check_overlapping_rates(rate_schedules):
        print("overlapping rate schedule found")


    for rate_schedule in rate_schedules:
        if rate_schedule["endTime"]<dates[0] or rate_schedule["startTime"] > dates[1]:
            continue
        updated_rate_schedule = dict()
        updated_rate_schedule["startTime"] = max(dates[0], rate_schedule["startTime"])
        updated_rate_schedule["endTime"] = min(dates[1], rate_schedule["endTime"])
        planNumber = rate_schedule["metaData"]["planNumber"]
        if planNumber in ['37', '34', '41', '42']:
            updated_rate_schedule["programId"] = tou_program_id
            
            updated_rate_schedule_2 = updated_rate_schedule.copy()
            
            updated_rate_schedule_2["programId"] = non_tou_program_id
            
        else:
            updated_rate_schedule["programId"] = non_tou_program_id
            
            updated_rate_schedule_2 = updated_rate_schedule.copy()
            
            updated_rate_schedule_2["programId"] = tou_program_id
            

        updated_rate_schedule["planNumber"] = planNumber
        updated_rate_schedule_2["planNumber"] = planNumber
        result.append(updated_rate_schedule)
        result.append(updated_rate_schedule_2)
    return result



def get_itemization(uuid):
    URL = "//v2.0/users/%s/endpoints" % uuid
    PARAMS = {"access_token": access_token}
    r = requests.get(url=URL, params=PARAMS)
    data = r.json()
    for item in data["payload"]:
        if item["measurementType"] == "Electricity":
            endpoint = item["endpointId"]
    
    URL = "/v2.0/users/%s/endpoints/%s/itemizationDetails?t0=1669870800&t1=1675227599&mode=month&isHybridQueryParam=true" % (uuid, endpoint)
    r = requests.get(url=URL, params=PARAMS)
    data = r.json()
    payload = data["payload"]
    result = []
    for item in payload:
        date = item["startDate"]
        ev_charge = ", EV COST - NA"
        for electricItem in item["electric"]:
            if electricItem["id"] == 18.0:
                ev_charge = ", EV COST - " + str(electricItem["cost"])
        result.append([date, ev_charge])
    return result


def get_total_cost(uuid, rate_plans, dates):
    cost = 0
    cost_map = dict()
    for rate in rate_plans:
        dates = [rate["startTime"], rate["endTime"]]
        plan_number = rate["planNumber"]
        program_id = rate["programId"]
        URL = "/billingdata/users/%s/homes/1/aggregatedCost/18/billing_cost?planNumber=%s&t0=%d&t1=%d&mode=day&tz=America/New_York&programId=%s" % (
            uuid, plan_number, dates[0], dates[1], program_id)
        PARAMS = {"access_token": access_token}

        r = requests.get(url=URL, params=PARAMS)
        data = r.json()
        if r.status_code != 200:
            continue
        for key, value in data.items():
            cost_map[key] = value["cost"]

    for key, value in cost_map.items():
        cost += value
    return cost


def get_tou_break_up(uuid, plan_number, dates):
    tou_map = {"Off-Peak": 0, "Mid-Peak": 0, "On-Peak": 0}
    tou_plan_number = dict()
    for plan in plan_number:
        if plan["planNumber"] in ['37', '34', '41', '42']:
            tou_plan_number = plan
            break
    if len(tou_plan_number) == 0:
        return tou_map
    URL = "/billingdata/users/%s/homes/1/aggregatedCost/18/tou?planNumber=%s&t0=%d&t1=%d&mode=month&tz=America/New_York" % (
    uuid, tou_plan_number["planNumber"], int(tou_plan_number["startTime"]), int(tou_plan_number["endTime"]))
    PARAMS = {"access_token": access_token}
    r = requests.get(url=URL, params=PARAMS)
    data = r.json()
    for key, value in data.items():
        tou_rrc_map = value["touAggData"]["touRrcMap"]
        for tou_name, tou_value in tou_rrc_map.items():
            tou_map[tou_name] = tou_map.get(tou_name) + tou_value["tierCons"]/1000
    print(tou_map)
    return tou_map


def get_uuids(uuid_file_path):
    uuid_set = set()
    file1 = open(uuid_file_path, 'r')
    uuids = file1.readlines()

    count = 0
    # Strips the newline character
    for line in uuids:
        count += 1
        uuid_set.add(line.strip())
    return uuid_set


def write_to_csv(header, data):
    with open(output_file_path, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)


def read_parquet(file_path, uuid_list_path, dates, is_tou=False, is_itemization=False):
    df = pd.read_parquet(file_path)
    agg_json = df.to_json()
    actual_json = json.loads(agg_json)
    uuids = actual_json["uuid"]
    tbdata = actual_json["totalTimeBandData"]
    data_gap_code = actual_json["dataGapCode"]
    header = ["UUID", "total_tb_data", "cost", "tou_breakup", "last_data_timestamp", "data_gap_code",  "itemization"]
    user_list = get_uuids(uuid_list_path)
    result = []
    executor = ThreadPoolExecutor(max_workers=100)
    count = 0
    for key, uuid in uuids.items():
        if uuid in user_list:
            timeband = tbdata[key]
            gap_code = data_gap_code[key]
            start = timeband["startTimestampList"]
            end = timeband["endTimestampList"]
            # print(end)
            value = timeband["valuesList"]
            total = 0
            last_data_timestamp = end[-1]
            # print(last_data_timestamp)
            for j in range(0, len(start)):
                total += ((end[j] - start[j]) / 900) * value[j]
            print("UUID: ", uuid, " total: ", total)
            if total > -1:
                count += 1
                result.append(executor.submit(fetch_data, uuid, total, dates, last_data_timestamp, gap_code, is_tou, is_itemization))
    done, not_done = wait(result, return_when=concurrent.futures.ALL_COMPLETED)
    print("Number of completed tasks: ", len(done), " and incomplete tasks: ", len(not_done))
    write_to_csv(header, data)
    print("Done execution for: ", count)


def fetch_data(uuid, total, dates, last_data_timestamp, data_gap_code, is_tou=False, is_itemization=False):
    plan_number = get_rate_plan_schedule(uuid, dates)
    try:
        cost = get_total_cost(uuid, plan_number, dates)

    except Exception as e:
        print("not success: ", uuid)
        print("Exception: ", e)
    itemization = list()
    if is_itemization:
        itemization = get_itemization(uuid)
    tou_map = dict()
    if is_tou:
        tou_map = get_tou_break_up(uuid, plan_number, dates)

    print("successfully processed user: ", uuid)
    temp_row = [uuid, total / 1000, cost, tou_map, last_data_timestamp, data_gap_code, itemization]
    data.append(temp_row)


def fetch_email_count(uuid):
    URL = "%s/2.1/utility_notifications/users/%s" % (url, uuid)
    PARAMS = {"access_token": access_token}
    r = requests.get(url=URL, params=PARAMS)
    data_s = r.json()
    result = []

    email_count = data_s["payload"]["totalCount"]
    temp_row=[uuid, email_count]
    data.append(temp_row)
    print([uuid, email_count])



if __name__ == '__main__':
    
    parquet_file_path_dec = ""
    uuid_list_path = "/Users/akhileshnegi/Documents/Work/PythonScript/upgradeReport/user.txt"
    # dec_interval = [1669870800, 1672549199]
    June_interval = [1690862400, 1693540799]
    output_file_path = '/Users/akhileshnegi/Documents/Pilot//Emails/MonthlyTracker/1_REPORT_2023/9Sept_AugReport/TOU/marketing/Sept_TOU_Marketing.csv'
    url = ""
    access_token = "2ebbd862-d93c-4036-8f85-37f5a380d025"

    read_parquet(parquet_file_path_dec, uuid_list_path, June_interval, is_tou=True, is_itemization=False)


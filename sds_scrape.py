import os
import requests
import datetime
import csv

sensor_ids = {
    "82312": {"start_date": "2023-08-10", "end_date": "2025-10-20"},
    "82328": {"start_date": "2023-08-18", "end_date": "2025-10-20"},
    "82334": {"start_date": "2023-08-11", "end_date": "2025-10-20"},
    "82338": {"start_date": "2023-08-29", "end_date": "2025-10-20"},
    "82342": {"start_date": "2023-08-29", "end_date": "2025-10-20"},
    "82344": {"start_date": "2023-08-24", "end_date": "2025-10-20"},
    "81967": {"start_date": "2024-07-07", "end_date": "2025-10-20"},
}
result_directory = "data"
log_file = "log.txt"
need_log = True
target_host = "https://archive.sensor.community/"
date_format = "%Y-%m-%d"
file_name_pattern = "_sds011_sensor_" + "[sensor_id]" + ".csv"

def str_to_date(date_str):
    return datetime.datetime.strptime(date_str, date_format).date()

def date_to_str(date_dte):
    return str(date_dte)

def getNextDate(date):
    return date + datetime.timedelta(days=1)

def log(text):
    if need_log:
        with open(log_file, "a") as file:
            file.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]) + ": " + text + "\n")

def prepare_result_directory(sensor_id):
    sensor_dir = os.path.join(result_directory, sensor_id)
    if not os.path.exists(sensor_dir):
        os.makedirs(sensor_dir)
    return sensor_dir

def getSensorDataForDate(date_str, fileName, detector_id):
    print("check data for date " + date_str + " and detector_id = " + detector_id)
    fullFileName = date_str + fileName
    url = target_host + date_str + "/" + fullFileName
    response = requests.get(url)
    if response.status_code == 200:
        sensor_dir = prepare_result_directory(detector_id)
        sourceFileName = os.path.join(sensor_dir, fullFileName)
        with open(sourceFileName, "w") as file:
            file.write(response.text)
        log("Data for date " + date_str + " from detector " + detector_id + " have been downloaded")
    else:
        log("Data for date " + date_str + " and detector_id " + detector_id + " not found")

def main():
    for id, dates in sensor_ids.items():
        start_date_dte = str_to_date(dates["start_date"])
        end_date_dte = str_to_date(dates["end_date"])
        if start_date_dte > end_date_dte:
            raise Exception("Error! Start Date can not be after lastDate for sensor " + id)
        fileName = file_name_pattern.replace("[sensor_id]", id)
        tmp_date_dte = start_date_dte
        while tmp_date_dte <= end_date_dte:
            tmp_date_str = date_to_str(tmp_date_dte)
            getSensorDataForDate(tmp_date_str, fileName, id)
            tmp_date_dte = getNextDate(tmp_date_dte)

if __name__ == '__main__':
    main()
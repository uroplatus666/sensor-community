import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import uuid
import dateutil.parser

# Configuration
BASE_URL = "http://localhost:8080/FROST-Server/v1.1"  # Change to your FrostServer url

HEADERS = {"Content-Type": "application/json"}
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
file_handler = logging.FileHandler("frost_upload.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
logging.getLogger().addHandler(file_handler)
created_ids = {"Things": [], "Sensors": [], "Datastreams": [], "Locations": [], "HistoricalLocations": [], "ObservedProperties": [], "FeaturesOfInterest": []}

# Sensor date ranges
sds_sensor_ids = {
    "82312": {"start_date": "2023-08-10", "end_date": "2025-10-20"}
}

bme_sensor_ids = {
    "82313": {"start_date": "2023-08-10", "end_date": "2025-10-20"}
}

def check_existing(endpoint, filter_str):
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}?$filter={filter_str}")
        if response.status_code == 200:
            data = response.json()
            if data.get("value") and len(data["value"]) > 0:
                return data["value"][0]["@iot.id"]
    except (requests.exceptions.RequestException, ValueError) as e:
        logging.error(f"Error checking existing {endpoint} with filter {filter_str}: {e}")
    return None

def post_entity(endpoint, data, dry_run=False):
    name = data.get("name", "")
    if "name" in data:
        existing_id = check_existing(endpoint, f"name eq '{name}'")
        if existing_id:
            logging.info(f"Existing {endpoint} found with name {name}: {existing_id}")
            return existing_id
    logging.info(f"Posting to {endpoint}: {json.dumps(data, indent=2, ensure_ascii=False)}")
    if dry_run:
        logging.info("DRY RUN: Simulated @iot.id")
        return str(uuid.uuid4())
    try:
        response = requests.post(f"{BASE_URL}/{endpoint}", headers=HEADERS, json=data)
        logging.info(f"Response status: {response.status_code}, Headers: {response.headers}, Body: {response.text}")
        if response.status_code == 201:
            try:
                # ИСПРАВЛЕНИЕ: Проверяем заголовок location и тело ответа
                location = response.headers.get("location")
                if location:
                    id_ = location.split("(")[-1].rstrip(")")
                    logging.info(f"Created {endpoint} with @iot.id={id_} from location header")
                    created_ids[endpoint.split('(')[0]].append(id_)
                    return id_
                # Проверяем тело ответа
                try:
                    response_json = response.json()
                    id_ = response_json.get("@iot.id")
                    if id_:
                        logging.info(f"Created {endpoint} with @iot.id={id_} from response body")
                        created_ids[endpoint.split('(')[0]].append(id_)
                        return id_
                except ValueError:
                    logging.warning(f"No JSON response body for {endpoint}")
                logging.error(f"Failed to extract @iot.id from response: Headers={response.headers}, Body={response.text}")
                return None
            except Exception as e:
                logging.error(f"Failed to extract @iot.id from response: {e}, Headers={response.headers}, Body={response.text}")
                return None
        else:
            logging.error(f"Error creating {endpoint}: Status {response.status_code}, Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for {endpoint}: {e}")
        return None

def post_batch(data_list, dry_run=False):
    if not data_list:
        logging.warning("Empty batch request, skipping")
        return []
    ids = []
    for i, data in enumerate(data_list):
        endpoint = data["path"].lstrip("/v1.1/")
        body = data["body"]
        id_ = post_entity(endpoint, body, dry_run)
        if id_:
            ids.append(id_)
        else:
            logging.error(f"Failed to post batch item {i+1} to {endpoint}")
    return ids

def delete_entity(endpoint, id_):
    try:
        response = requests.delete(f"{BASE_URL}/{endpoint}({id_})")
        if response.status_code == 200:
            logging.info(f"Deleted {endpoint}({id_})")
        else:
            logging.error(f"Error deleting {endpoint}({id_}): {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to delete {endpoint}({id_}): {e}")

def save_ids(ids_dict, filename="ids_log.json"):
    try:
        with open(filename, "a", encoding="utf-8") as f:
            json.dump(ids_dict, f, indent=2, ensure_ascii=False)
            f.write("\n")
    except IOError as e:
        logging.error(f"Error saving IDs to {filename}: {e}")

def create_observed_properties(dry_run=False):
    obs_props = [
        {"name": "Относительная влажность воздуха", "description": "Relative humidity in percent", "definition": "http://dbpedia.org/page/Humidity"},
        {"name": "Температура воздуха", "description": "Air temperature in Celsius", "definition": "http://dbpedia.org/page/Temperature"},
        {"name": "Атмосферное давление", "description": "Atmospheric pressure in Pa", "definition": "http://dbpedia.org/page/Atmospheric_pressure"},
        {"name": "PM2.5", "description": "Concentration of particulate matter with diameter up to 2.5 micrometers in µg/m³", "definition": "http://dbpedia.org/page/Particulates"},
        {"name": "PM10", "description": "Concentration of particulate matter with diameter up to 10 micrometers in µg/m³", "definition": "http://dbpedia.org/page/Particulates"},
    ]
    obs_prop_ids = {}
    for prop in obs_props:
        id_ = post_entity("ObservedProperties", prop, dry_run)
        if id_:
            obs_prop_ids[prop["name"]] = id_
    return obs_prop_ids

def process_group(group, obs_prop_ids, dry_run=False):
    try:
        inv = group['Инвентарный номер изделия'].iloc[0]
        brand = group['Марка'].iloc[0]
        proc_id = group['Номер процессора'].iloc[0]
        type_ = group['Тип'].iloc[0]
    except (IndexError, KeyError) as e:
        logging.error(f"Invalid data in group for inventory {group.get('Инвентарный номер изделия', 'unknown')}: {e}")
        return None, None, None, None, None, None, None

    # Create Thing
    thing_data = {
        "name": f"Стационарный датчик пыли {inv}",
        "description": "SDS011+BME280",
        "properties": {
            "brand": str(brand),
            "processorId": str(proc_id),
            "type": str(type_)
        }
    }
    thing_id = post_entity("Things", thing_data, dry_run)
    if not thing_id:
        logging.error(f"Failed to create Thing for inventory {inv}")
        return None, None, None, None, None, None, None

    # Find SDS and BME sensor_ids
    sds_rows = group[group['sensor_type'] == 'SDS011']
    bme_rows = group[group['sensor_type'] == 'BME280']
    if sds_rows.empty or bme_rows.empty:
        logging.error(f"Missing SDS011 or BME280 sensor for inventory {inv}")
        return None, None, None, None, None, None, None

    try:
        sds_sensor_id = sds_rows['sensor_id'].iloc[0]
        bme_sensor_id = bme_rows['sensor_id'].iloc[0]
    except IndexError as e:
        logging.error(f"Error accessing sensor_id for inventory {inv}: {e}")
        return None, None, None, None, None, None, None

    # Create Sensors
    sds_sensor_data = {
        "name": f"SDS011_{inv}",
        "description": "Sensor for measuring the concentration of particulate matter with diameter up to 2.5 and up to 10 micrometers in µg/m³",
        "encodingType": "application/pdf",
        "metadata": "https://nova-fitness.com/uploads/2021/11/SDS011-Datasheet.pdf"
    }
    sds_sensor_id_ = post_entity("Sensors", sds_sensor_data, dry_run)
    if not sds_sensor_id_:
        logging.error(f"Failed to create SDS011 Sensor for inventory {inv}")
        return None, None, None, None, None, None, None

    bme_sensor_data = {
        "name": f"BME280_{inv}",
        "description": "Sensor for measuring temperature in °C, relative humidity in % and atmospheric pressure in Pa",
        "encodingType": "application/pdf",
        "metadata": "https://www.bosch-sensortec.com/media/boschsensortec/downloads/datasheets/bst-bme280-ds002.pdf"
    }
    bme_sensor_id_ = post_entity("Sensors", bme_sensor_data, dry_run)
    if not bme_sensor_id_:
        logging.error(f"Failed to create BME280 Sensor for inventory {inv}")
        return None, None, None, None, None, None, None

    # Create Locations and HistoricalLocations
    foi_ids = {}
    # ИСПРАВЛЕНИЕ: Проверка уникальности строк в группе
    unique_locations = group[['address', 'lon', 'lat', 'first_seen']].drop_duplicates().reset_index(drop=True)
    logging.info(f"Processing {len(unique_locations)} unique locations for inventory {inv}")
    for _, row in unique_locations.iterrows():
        # ИСПРАВЛЕНИЕ: Валидация данных местоположения
        try:
            lon = float(row['lon'])
            lat = float(row['lat'])
            address = str(row['address'])
            first_seen = str(row['first_seen'])
            # Преобразование first_seen в ISO 8601 с часовым поясом
            try:
                parsed_time = dateutil.parser.isoparse(first_seen)
                first_seen = parsed_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                logging.error(f"Invalid first_seen format for inventory {inv}: {first_seen}")
                continue
        except (ValueError, TypeError) as e:
            logging.error(f"Invalid location data for inventory {inv}: {e}")
            continue

        loc_data = {
            "name": address,
            "description": f"Location for sensor {inv}",
            "encodingType": "application/vnd.geo+json",
            "location": {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        }
        loc_id = post_entity("Locations", loc_data, dry_run)
        if not loc_id:
            logging.error(f"Failed to create Location for inventory {inv} at {address}")
            continue

        # ИСПРАВЛЕНИЕ: Модифицированный фильтр для HistoricalLocations
        hist_filter = f"time eq '{first_seen}' and Thing/@iot.id eq {thing_id} and Locations/any(l:l/location/coordinates eq '[{lon},{lat}]')"
        existing_hist = check_existing("HistoricalLocations", hist_filter)
        if existing_hist:
            logging.info(f"Existing HistoricalLocation found for time {first_seen}, Thing {thing_id}, Location {loc_id}: {existing_hist}")
            continue
        hist_data = {
            "time": first_seen,
            "Thing": {"@iot.id": thing_id},
            "Locations": [{"@iot.id": loc_id}]
        }
        logging.info(f"Attempting to create HistoricalLocation for inventory {inv} at time {first_seen} and location {address}")
        hist_id = post_entity("HistoricalLocations", hist_data, dry_run)
        if not hist_id:
            logging.error(f"Failed to create HistoricalLocation for inventory {inv} at time {first_seen} and location {address}")
        else:
            logging.info(f"Successfully created HistoricalLocation with @iot.id={hist_id}")
            created_ids["HistoricalLocations"].append(hist_id)

        # Create FeatureOfInterest
        foi_data = {
            "name": f"FOI_{inv}_{address}",
            "description": f"Feature of interest for sensor {inv} at {address}",
            "encodingType": "application/vnd.geo+json",
            "feature": {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        }
        foi_id = post_entity("FeaturesOfInterest", foi_data, dry_run)
        if foi_id:
            foi_ids[address] = foi_id

    # Create Datastreams
    datastreams = [
        {
            "name": f"PM2.5_{inv}",
            "description": "PM2.5 concentration in µg/m³",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "microgram per cubic meter",
                "symbol": "µg/m³",
                "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#MicrogramPerCubicMeter"
            },
            "Thing": {"@iot.id": thing_id},
            "Sensor": {"@iot.id": sds_sensor_id_},
            "ObservedProperty": {"@iot.id": obs_prop_ids["PM2.5"]}
        },
        {
            "name": f"PM10_{inv}",
            "description": "PM10 concentration in µg/m³",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "microgram per cubic meter",
                "symbol": "µg/m³",
                "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#MicrogramPerCubicMeter"
            },
            "Thing": {"@iot.id": thing_id},
            "Sensor": {"@iot.id": sds_sensor_id_},
            "ObservedProperty": {"@iot.id": obs_prop_ids["PM10"]}
        },
        {
            "name": f"Temperature_{inv}",
            "description": "Air temperature in Celsius",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "degree Celsius",
                "symbol": "°C",
                "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"
            },
            "Thing": {"@iot.id": thing_id},
            "Sensor": {"@iot.id": bme_sensor_id_},
            "ObservedProperty": {"@iot.id": obs_prop_ids["Температура воздуха"]}
        },
        {
            "name": f"Humidity_{inv}",
            "description": "Relative humidity in percent",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "percent",
                "symbol": "%",
                "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#Percent"
            },
            "Thing": {"@iot.id": thing_id},
            "Sensor": {"@iot.id": bme_sensor_id_},
            "ObservedProperty": {"@iot.id": obs_prop_ids["Относительная влажность воздуха"]}
        },
        {
            "name": f"Pressure_{inv}",
            "description": "Atmospheric pressure in Pa",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "hectopascal",
                "symbol": "Pa",
                "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#Hectopascal"
            },
            "Thing": {"@iot.id": thing_id},
            "Sensor": {"@iot.id": bme_sensor_id_},
            "ObservedProperty": {"@iot.id": obs_prop_ids["Атмосферное давление"]}
        }
    ]
    batch_request = [{"request": "POST", "path": "/v1.1/Datastreams", "body": ds} for ds in datastreams]
    ids = post_batch(batch_request, dry_run)
    if not ids or len(ids) < 5:
        logging.error(f"Failed to create Datastreams for inventory {inv}: Got {len(ids)} IDs")
        return None, None, None, None, None, None, None

    datastream_ids = {
        "P2": ids[0],
        "P1": ids[1],
        "temperature": ids[2],
        "humidity": ids[3],
        "pressure": ids[4]
    }
    save_ids({"thing_id": thing_id, "sds_sensor_id": sds_sensor_id_, "bme_sensor_id": bme_sensor_id_, "datastream_ids": datastream_ids, "foi_ids": foi_ids})
    return thing_id, sds_sensor_id_, bme_sensor_id_, datastream_ids, sds_sensor_id, bme_sensor_id, foi_ids

def upload_observations(sensor_id, datastream_ids, sensor_type, start_date, end_date, foi_ids, dry_run=False):
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        logging.error(f"Invalid date format for sensor {sensor_id}: {e}")
        return

    current = start
    observations = []
    observation_count = 0
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        if sensor_type == "SDS011":
            csv_path = os.path.join("data", "SDS011", str(sensor_id), f"{date_str}_sds011_sensor_{sensor_id}.csv")
            columns = ["P1", "P2"]
            ds_keys = ["P1", "P2"]
        else:  # BME280
            csv_path = os.path.join("data", "BME280", str(sensor_id), f"{date_str}_bme280_sensor_{sensor_id}.csv")
            columns = ["temperature", "humidity", "pressure"]
            ds_keys = ["temperature", "humidity", "pressure"]

        if os.path.exists(csv_path):
            logging.info(f"Reading CSV: {csv_path}")
            try:
                csv_df = pd.read_csv(csv_path, sep=";", engine='python')
                if csv_df.empty:
                    logging.warning(f"Empty CSV file: {csv_path}")
                    current += timedelta(days=1)
                    continue
                logging.info(f"CSV columns: {list(csv_df.columns)}")
                if "timestamp" not in csv_df.columns:
                    logging.error(f"Missing 'timestamp' column in CSV: {csv_path}")
                    current += timedelta(days=1)
                    continue
                for _, row in csv_df.iterrows():
                    try:
                        # Validate and format timestamp
                        timestamp = str(row["timestamp"])
                        try:
                            parsed_time = dateutil.parser.isoparse(timestamp)
                            formatted_time = parsed_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            logging.error(f"Invalid timestamp format in CSV {csv_path}: {timestamp}")
                            continue
                        for col, ds_key in zip(columns, ds_keys):
                            if col in csv_df.columns and pd.notna(row[col]):
                                try:
                                    result = float(row[col])
                                    # Use the first FeatureOfInterest ID (assumes one primary location per sensor)
                                    foi_id = list(foi_ids.values())[0] if foi_ids else None
                                    if not foi_id:
                                        logging.error(f"No FeatureOfInterest ID available for sensor {sensor_id}")
                                        continue
                                    obs = {
                                        "phenomenonTime": formatted_time,
                                        "result": result,
                                        "Datastream": {"@iot.id": datastream_ids[ds_key]},
                                        "FeatureOfInterest": {"@iot.id": foi_id}
                                    }
                                    observations.append(obs)
                                    observation_count += 1
                                    if observation_count % 100 == 0:
                                        logging.info(f"Prepared observation #{observation_count} for sensor {sensor_id}: {json.dumps(obs, indent=2)}")
                                except ValueError:
                                    logging.error(f"Invalid result value for {col} in CSV {csv_path}: {row[col]}")
                    except KeyError:
                        logging.error(f"Missing required field in CSV {csv_path}: {row}")
                if len(observations) >= 100:
                    batch_request = [{"request": "POST", "path": "/v1.1/Observations", "body": obs} for obs in observations]
                    post_batch(batch_request, dry_run)
                    observations = []
            except (pd.errors.ParserError, KeyError, ValueError) as e:
                logging.error(f"Error reading CSV {csv_path}: {e}")
        else:
            logging.warning(f"CSV file not found: {csv_path}")

        current += timedelta(days=1)

    if observations:
        batch_request = [{"request": "POST", "path": "/v1.1/Observations", "body": obs} for obs in observations]
        post_batch(batch_request, dry_run)
        logging.info(f"Posted final batch of {len(observations)} observations for sensor {sensor_id}")

def main(excel_path="all_stats.xlsx", dry_run=False):
    try:
        df = pd.read_excel(excel_path)
    except (FileNotFoundError, ValueError) as e:
        logging.error(f"Error reading Excel file {excel_path}: {e}")
        return

    required_columns = ['Инвентарный номер изделия', 'Марка', 'Номер процессора', 'Тип', 'sensor_type', 'sensor_id', 'address', 'lon', 'lat', 'first_seen']
    if not all(col in df.columns for col in required_columns):
        logging.error(f"Missing required columns in {excel_path}: {required_columns}")
        return

    # ИСПРАВЛЕНИЕ: Логирование содержимого групп для диагностики
    logging.info("Checking group data:")
    groups = df.groupby('Инвентарный номер изделия')
    for inv, g in groups:
        logging.info(f"Inventory {inv}:")
        logging.info(g[['address', 'lon', 'lat', 'first_seen']].to_string())

    obs_prop_ids = create_observed_properties(dry_run)
    if not obs_prop_ids:
        logging.error("Failed to create ObservedProperties")
        return

    for inv, group in groups:
        result = process_group(group, obs_prop_ids, dry_run)
        thing_id, sds_sensor_id_, bme_sensor_id_, datastream_ids, sds_sensor_id_str, bme_sensor_id_str, foi_ids = result
        if datastream_ids:
            if str(sds_sensor_id_str) in sds_sensor_ids:
                sds_range = sds_sensor_ids[str(sds_sensor_id_str)]
                upload_observations(sds_sensor_id_str, {"P1": datastream_ids["P1"], "P2": datastream_ids["P2"]}, "SDS011", sds_range["start_date"], sds_range["end_date"], foi_ids, dry_run)
            if str(bme_sensor_id_str) in bme_sensor_ids:
                bme_range = bme_sensor_ids[str(bme_sensor_id_str)]
                upload_observations(bme_sensor_id_str, {"temperature": datastream_ids["temperature"], "humidity": datastream_ids["humidity"], "pressure": datastream_ids["pressure"]}, "BME280", bme_range["start_date"], bme_range["end_date"], foi_ids, dry_run)

if __name__ == "__main__":
    main(dry_run=False)
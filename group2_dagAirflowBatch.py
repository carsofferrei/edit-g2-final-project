from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime
import os
import subprocess
import requests
import json
from google.cloud import storage, bigquery
import pandas as pd
from google.api_core.exceptions import NotFound
import zipfile
from urllib.parse import urlparse
import shutil


bucket_name = 'edit-data-eng-project-group2'
bucket = storage.Client().get_bucket(bucket_name)

# Get data from api url
def get_data_from_api(url, headers=None):
    try:
        response = requests.get(url, headers=headers)

        content_type = response.headers.get('Content-Type', '')

        if 'application/json' in content_type:
            data = response.json()
            if url == 'https://api.ipma.pt/public/opendata/weatherforecast/daily/1110600.json':
                upload_weather_data(data)
            elif data:
                upload_carris_data(data, url)
        else:
            # Content type unknown
            raise ValueError(f"Unsupported content type: {content_type}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")


# Get JSON file saved locally to upload it to bucket in GCS
def upload_files_to_bucket(bucket_name, source_file, destination_file):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_file)

    # In case the file contains weather data , aggregate new weather data
    if destination_file == "api_weather_forecast_cleaned.json":
        if blob.exists():
            # Download of file existent in bucket
            existing_data = json.loads(blob.download_as_text())

            # Load new data
            with open(source_file, 'r') as f:
                new_data = json.load(f)

            # Aggregate data
            if isinstance(existing_data.get("data"), list) and isinstance(new_data.get("data"), list):
                combined_data = {
                    "owner": existing_data["owner"],
                    "country": existing_data["country"],
                    "data": existing_data["data"] + [item for item in new_data["data"]]
                }
            else:
                raise ValueError("Data format is not compatible")

            # Do upload of data
            with open(source_file, 'w') as f:
                json.dump(combined_data, f, indent=4)

            blob.upload_from_filename(source_file)
            print(f"Data aggregated and uploaded to {destination_file}.")
        else:
            # If file does not exist, upload data to bucket
            blob.upload_from_filename(source_file)
            print(f"File {source_file} uploaded to {destination_file}.")

    blob.upload_from_filename(source_file)
    print(f"File {source_file} uploaded to {destination_file}.")


def download_file_from_bucket(bucket_name, source_file, destination_file):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_file)

    blob.download_to_filename(destination_file)
    print(f"Downloaded {source_file} from bucket {bucket_name} to {destination_file}")

# Remove null values from JSON file
def remove_null_values(data):
    if isinstance(data, list):
        # If it is a list, applies this function to every object of the list
        return [remove_null_values(item) for item in data if item is not None]
    elif isinstance(data, dict):
        # If it is a dictionary, removes keys with values equal to null
        return {key: remove_null_values(value) for key, value in data.items() if value is not None}
    else:
        # Return value if it's not a list or a dictionary
        return data

# Process JSON file to give it a first cleaning and save it again without null values
def process_json_file(input_file, output_file):
    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        for line in infile:
            data = json.loads(line)

            # Remove null values
            cleaned_data = remove_null_values(data)

            # Writes cleaned JSON into file
            json.dump(cleaned_data, outfile)
            outfile.write("\n")

# Remove local files after processing
def clean_local_files(files):
    for file in files:
        try:
            os.remove(file)
            print(f"File {file} removed successfully")
        except FileNotFoundError:
            print(f"File {file} not found, skipping.")
        except Exception as e:
            print(f"An error occurred while deleting {file}: {e}")

def upload_carris_data(data, url):
    # Parse a URL
    parsed_url = urlparse(url)
    # Extract Path
    path = parsed_url.path

    # Handle specific structure for "alerts"
    if path == "alerts" and isinstance(data, dict):
        # Extract entities for alerts
        header = data.get("header", {})
        entities = data.get("entity", [])

        # Include header as a single record
        entities.insert(0, {"header": header})

    else:
        # Assume data is already a list
        entities = data

    json_object = "\n".join([json.dumps(record) for record in entities])

    safe_path = path.replace("/", "_")
    # Initial JSON file
    data_json = "api" + safe_path + ".json"
    # JSON file cleaned
    data_json_cleaned = "api" + safe_path + "_cleaned.json"

    with open(data_json, "w") as outfile:
        outfile.write(json_object)

    process_json_file(data_json, data_json_cleaned)

    # Upload json to bucket in GCS
    upload_files_to_bucket(
        bucket_name=bucket_name,
        source_file=data_json_cleaned,
        destination_file=data_json_cleaned,
    )

    # Remove local files
    clean_local_files([data_json, data_json_cleaned])

def upload_weather_data(weather_data):
    cleaned_weather_data = remove_null_values(weather_data)

    # Save data locally
    weather_json = "api_weather_forecast.json"
    weather_json_cleaned = "api_weather_forecast_cleaned.json"

    with open(weather_json, "w") as outfile:
        json.dump(cleaned_weather_data, outfile)

    process_json_file(weather_json, weather_json_cleaned)

    # Upload of JSON to bucket
    upload_files_to_bucket(
        bucket_name=bucket_name,
        source_file=weather_json_cleaned,
        destination_file=weather_json_cleaned,
    )

    clean_local_files([weather_json, weather_json_cleaned])

def download_and_upload_gtfs():
    # URL
    url = "https://api.carrismetropolitana.pt/gtfs"
    zip_file = "carris_metropolitana_gtfs.zip"
    extract_folder = "carris_metropolitana_gtfs"

    # Always download the latest zip file
    response = requests.get(url)
    if response.status_code == 200:
        with open(zip_file, "wb") as f:
            f.write(response.content)
        print("GTFS ZIP file downloaded successfully.")
    else:
        raise Exception(f"Failed to download the GTFS file. Status code: {response.status_code}")

    # Extract content in case those are not extracted
    if os.path.exists(extract_folder):
        shutil.rmtree(extract_folder)
        print(f"Existing folder {extract_folder} removed.")
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
        print(f"Extracted contents to {extract_folder}")

    # Upload of files to bucket
    for file_name in os.listdir(extract_folder):
        if file_name.endswith(".txt"):
            file_path = os.path.join(extract_folder, file_name)
            upload_files_to_bucket(
                bucket_name=bucket_name,
                source_file=file_path,
                destination_file=file_name,
            )
            print(f"Uploaded {file_name} to bucket.")

def get_historical_data_carris(desired_file):
    # Bucket Path
    bucket_path = f"gs://{bucket_name}/"

    # Download of file desired from bucket
    file_name = f"{desired_file}.txt"
    local_file_path = f"./{file_name}"
    download_file_from_bucket(
        bucket_name=bucket_name,
        source_file=file_name,
        destination_file=local_file_path,
    )
    print(f"Downloaded {file_name} from bucket.")

    # Process file to store as CSV
    df = pd.read_csv(local_file_path, dtype=str, low_memory=False)
    if file_name == "stop_times.txt":
        df['arrival_time'] = df['arrival_time'].str.replace(r'^24:', '00:', regex=True)
        df['departure_time'] = df['departure_time'].str.replace(r'^24:', '00:', regex=True)
        df['arrival_time'] = df['arrival_time'].str.replace(r'^25:', '01:', regex=True)
        df['departure_time'] = df['departure_time'].str.replace(r'^25:', '01:', regex=True)
        df['arrival_time'] = df['arrival_time'].str.replace(r'^26:', '02:', regex=True)
        df['departure_time'] = df['departure_time'].str.replace(r'^26:', '02:', regex=True)
        df['arrival_time'] = df['arrival_time'].str.replace(r'^27:', '03:', regex=True)
        df['departure_time'] = df['departure_time'].str.replace(r'^27:', '03:', regex=True)
        df['arrival_time'] = df['arrival_time'].str.replace(r'^28:', '04:', regex=True)
        df['departure_time'] = df['departure_time'].str.replace(r'^28:', '04:', regex=True)
        df['arrival_time'] = df['arrival_time'].str.replace(r'^29:', '05:', regex=True)
        df['departure_time'] = df['departure_time'].str.replace(r'^29:', '05:', regex=True)
        df['arrival_time'] = df['arrival_time'].str.replace(r'^30:', '06:', regex=True)
        df['departure_time'] = df['departure_time'].str.replace(r'^30:', '06:', regex=True)


    csv_file_path = f"{desired_file}.csv"
    df.to_csv(csv_file_path, index=False, encoding="utf-8")
    print(f"Saved {desired_file} DataFrame to {csv_file_path}")

    # Do upload of CSV to bucket
    upload_files_to_bucket(
        bucket_name=bucket_name,
        source_file=csv_file_path,
        destination_file=csv_file_path,
    )
    print(f"Uploaded {csv_file_path} to bucket as {desired_file}.")


def load_data_to_bigQuery(uri, path):

    client = bigquery.Client()

    pathTest = path

    # Detect the file format based on the extension
    file_extension = os.path.splitext(path)[1].lower()

    if file_extension == ".json":
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    elif file_extension == ".csv":
        source_format = bigquery.SourceFormat.CSV
    else:
        raise ValueError(f"Unsupported file extension: {file_extension}")

    # Configure job settings
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=source_format,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    path_without_extension = os.path.splitext(path)[0]

    table = path_without_extension

    # Name of table in BigQuery
    table_id = "data-eng-dev-437916.data_eng_project_group2." + table

    try:
        # Upload file into BigQuery
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            location="EU",
            job_config=job_config,
        )

        assert load_job.job_type == "load"

        # Waits for the job to complete
        load_job.result()
        print(f"Table {table_id} loaded with success")
    except Exception as e:
        print(f"Error loading data to {table_id}: {e}")



# DAG
dag = DAG (
    'group2_api_to_bigQuery_dag',
    description = 'DAG to collect data from multiple sources into BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 9),
    catchup=False,

)

# Download and upload of GTFS
download_and_upload_task = PythonOperator(
    task_id='download_and_upload_gtfs',
    python_callable=download_and_upload_gtfs,
    dag=dag,
)

# API vehicles
endpoint_carris = "https://api.carrismetropolitana.pt/"
endpoints_carris_paths = ["alerts", "municipalities", "stops", "lines", "routes", "datasets/facilities/encm",
                    "datasets/facilities/schools"]

# Get data from multiple apis from carris
endpoints_url = []
for path in endpoints_carris_paths:
    endpoints_url.append(endpoint_carris + path)

#Get data of weather from API of IPMA
weather_url = "https://api.ipma.pt/public/opendata/weatherforecast/daily/1110600.json"
endpoints_url.append(weather_url)

tasks_fetching_api = []
for index, endpoint in enumerate(endpoints_url):

    # Check if it's a Carris endpoint
    if index < len(endpoints_carris_paths):
        endpoint_path = endpoints_carris_paths[index]
    else:
        # Handle the weather API or other non-Carris endpoints
        endpoint_path = "weather"

    # Task to execute script gettingDataFromApi
    safe_endpoint_path = endpoint_path.replace("/", "_")
    task1 = PythonOperator(
        task_id=f'get_data_from_api_{safe_endpoint_path}',
        python_callable=get_data_from_api,
        op_args=[endpoint],
        dag=dag,
    )

    tasks_fetching_api.append(task1)

tasks_fetching_historical = []
desired_files = {"calendar_dates", "stop_times", "trips"}
for desired_file in desired_files:
    task3 = PythonOperator(
            task_id=f'get_historical_data_carris_{desired_file}',
            python_callable=get_historical_data_carris,
            op_args=[desired_file],
            dag=dag,
        )
    task3.set_upstream(download_and_upload_task)
    tasks_fetching_historical.append(task3)

#tasks_fetching = tasks_fetching_api + tasks_fetching_historical

endpoint_bucket = "gs://edit-data-eng-project-group2/"
endpoints_bucket_paths = ["api_alerts_cleaned.json", "api_municipalities_cleaned.json", "api_stops_cleaned.json",
                    "api_lines_cleaned.json", "api_routes_cleaned.json", "api_datasets_facilities_encm_cleaned.json",
                    "api_datasets_facilities_schools_cleaned.json", "api_weather_forecast_cleaned.json"]

tasks_loading = []
# Loop through every json present in bucket from GCS
for index, path in enumerate(endpoints_bucket_paths):
    uri_bucket = endpoint_bucket + path

    # Task to execute script loadingIntoBigQuery
    task2 = PythonOperator (
        task_id=f'load_data_to_bigQuery_{path}',
        python_callable=load_data_to_bigQuery,
        op_args=[uri_bucket, path],
        dag=dag,
    )

    tasks_loading.append(task2)

endpoints_bucket_paths_historical = ["calendar_dates.csv", "stop_times.csv", "trips.csv"]
tasks_loading_historical = []
# Loop through every csv present in bucket from GCS
for index, path in enumerate(endpoints_bucket_paths_historical):
    uri_bucket_historical = endpoint_bucket + path

    # Task to execute script loadingIntoBigQuery
    task4 = PythonOperator (
        task_id=f'load_csv_data_to_bigQuery_{path}',
        python_callable=load_data_to_bigQuery,
        op_args=[uri_bucket_historical, path],
        dag=dag,
    )

    tasks_loading_historical.append(task4)


dbt_run = CloudRunExecuteJobOperator(
    task_id='dbt_run',
    project_id='data-eng-dev-437916',
    region='europe-west1',
    job_name='edit-data-eng-project-group2-dbt',
    overrides={
        "container_overrides": [
            {
                "args": ["run"]
            }
        ]
    }
)


dbt_test = CloudRunExecuteJobOperator(
    task_id='dbt_test',
    project_id='data-eng-dev-437916',
    region='europe-west1',
    job_name='edit-data-eng-project-group2-dbt',
    overrides={
        "container_overrides": [
            {
                "args": ["test"]
            }
        ]
    }
)


#Order of execution
for tasks_fetching_api, task_loading in zip(tasks_fetching_api, tasks_loading):
    tasks_fetching_api >> task_loading


#Order of execution csvs
#for task_fetching_csv, task_loading_csv in zip(tasks_fetching_historical, tasks_loading_historical):
#    task_fetching_csv >> task_loading_csv

for task_fetching_csv in tasks_fetching_historical:
    for task_loading_csv in tasks_loading_historical:
        task_fetching_csv >> task_loading_csv


# Linking DBT tasks to the data loading process
for task in tasks_loading + tasks_loading_historical:
    task >> dbt_run

dbt_run >> dbt_test

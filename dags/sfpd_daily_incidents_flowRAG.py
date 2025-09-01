#  airflow imports
# from airflow.decorators import dag, task

# utility imports
import requests
from datetime import date, timedelta
import os

# configs
SODA_ENDPOINT = "https://data.sfgov.org/resource/wg3w-h783.json"
SODA_NUM_RECORDS = 10000       # how many records to get
SODA_NUM_DAYS_OF_RECORDS = 14       # how many days of records (going back from today) to get
SODA_ENDDATE = date.today() - timedelta(days=int(f"{SODA_NUM_DAYS_OF_RECORDS}"))     # end date of records
SODA_CLAUSE = f"$limit={SODA_NUM_RECORDS}&$where=incident_date%20not%20between%20%272018-01-01%27%20and%20%27{SODA_ENDDATE}%27"

DB_CONN = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

@dag(
    dag_id="sfpd_daily_incidents_flowRAG_daily_taskflow",
    schedule="0 10 * * *"
)
def pipeline():
    @task
    def extract():
        url = f"{SODA_ENDPOINT}?{SODA_CLAUSE}"
        KEYS_TO_KEEP = {        # which keys to keep from the raw data
        "incident_id",
        "incident_date",
        "incident_time",
        "incident_year",
        "incident_day_of_week",
        "incident_category",
        "incident_subcategory",
        "incident_description",
        "resolution",
        "police_district",
        "analysis_neighborhood",
        "intersection",
        "latitude",
        "longitude"
        }
        raw_data = requests.get(url).json()
        trimmed_data = [        # keep only the useful keys of the raw data
            {k:d[k] for k in KEYS_TO_KEEP if k in d} for d in raw_data
        ]
        print(trimmed_data)
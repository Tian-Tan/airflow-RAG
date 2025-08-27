#  airflow imports
from airflow.decorators import dag, task

# utility imports
import requests
from datetime import date, timedelta

# configs
SODA_ENDPOINT = "https://data.sfgov.org/resource/wg3w-h783.json"
SODA_NUM_RECORDS = 10000       # how many records to get
SODA_NUM_DAYS_OF_RECORDS = 14       # how many days of records (going back from today) to get
SODA_ENDDATE = date.today() - timedelta(days=int(f"{SODA_NUM_DAYS_OF_RECORDS}"))     # end date of records
SODA_CLAUSE = f"$limit={SODA_NUM_RECORDS}&$where=incident_date%20not%20between%20%272018-01-01%27%20and%20%27{SODA_ENDDATE}%27"

@dag(
    dag_id="sfpd_daily_incidents_flowRAG_daily_taskflow",
    schedule="0 10 * * *"
)
def pipeline():
    @task
    def extract():
        url = f"{SODA_ENDPOINT}?{SODA_CLAUSE}"
        print(requests.get(url).json())
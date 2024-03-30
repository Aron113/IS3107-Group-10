from airflow.decorators import dag, task
from datetime import datetime, timedelta
import json
import pandas as pd
import stscraping

#Connecting to BQ table to insert data
from google.cloud import bigquery
from google.oauth2 import service_account


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1), #setting start date to 1st Jan 2024
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}


credentials = service_account.Credentials.from_service_account_file('IS3107 Keys.json')
client = bigquery.Client(credentials=credentials)
            
@dag(dag_id='stscraping_insertion', default_args=default_args, schedule=None, catchup=False, tags=['IS3107_Project'])
def project():

    @task
    def scrape_st():
        df = stscraping.scrape_st()
        if df is not None:
            print(df.head())
        return df
    
    @task
    def data_insertion(df):
        dataset_id = 'Dataset'
        table_id = 'Straitstimes Data'
        
        data = df

        if data is None:
            print("Data is none")
            return
        
        # Create the BigQuery dataset if it doesn't exist
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        try:
            dataset = client.create_dataset(dataset)  # Will raise an exception if dataset already exists
        except Exception as e:
            pass  # Dataset already exists

        # Create the BigQuery table if it doesn't exist
        table_ref = dataset_ref.table(table_id)
        #table = bigquery.Table(table_ref, schema=schema)
        try:
            table = client.create_table(table)  # Will raise an exception if table already exists
        except Exception as e:
            pass  # Table already exists

        # Load data into the BigQuery table
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)

        # Wait for the job to complete
        job.result()

        print('Data successfully loaded into BigQuery table.')

    
    df = scrape_st()
    data_insertion(df)


Project_DAG = project()

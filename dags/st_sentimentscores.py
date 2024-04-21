from airflow.decorators import dag, task
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from textblob import TextBlob
import re

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1), #setting start date to 1st Jan 2024
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

keywords = ["government", "ministry", "budget", "GST", "CPF", "education", "healthcare", 
            "transport", "housing", "PAP", "elections", "ministers"]
keyword_pattern = re.compile(r'\b(?:' + '|'.join(keywords) + r')\b', flags=re.IGNORECASE)

credentials = service_account.Credentials.from_service_account_file('IS3107 Keys.json')
client = bigquery.Client(credentials=credentials)

@dag(dag_id='st_sentimentscores', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['IS3107_Project'])
def project():
    
    def preprocess_text(text):
        # Preprocess the text by removing special characters and converting to lowercase
        text = text.replace('\n', ' ').replace('\r', '')
        text = re.sub(r'[^\w\s]', '', text)
        text = text.lower()
        return text
    
    @task
    def perform_st_sentiment_analysis():
        query = f"""
            SELECT url, title, text
            FROM `is3107-group-10.Dataset.Straitstimes Data`
            WHERE REGEXP_CONTAINS(text, r'({"|".join(keywords)})')
        """

        results = client.query(query).to_dataframe()
        
        # Filter the results based on keywords
        results = results[results['text'].str.contains(keyword_pattern, na=False)]
        
        # Preprocess the text
        results['text'] = results['text'].apply(preprocess_text)

        # Perform sentiment analysis on the text
        sentiments = []
        for text in results['text']:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            sentiments.append({'polarity': polarity, 'subjectivity': subjectivity})
            
        # Add sentiment scores to the dataframe
        results["sentiments"] = sentiments
        return results
    
    @task
    def insert_sentiment_scores(results):
        if results is not None:
            # Insert the sentiment scores into the BigQuery table
            table_id = 'is3107-group-10.Dataset.Straitstimes Data with sentiment scores'
            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            client.load_table_from_dataframe(results, table_id, job_config=job_config).result()
            print("Sentiment scores inserted into BigQuery table")

    results = perform_st_sentiment_analysis()
    insert_sentiment_scores(results)
    
Project_DAG = project()

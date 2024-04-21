from airflow.decorators import dag, task
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
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


credentials = service_account.Credentials.from_service_account_file('IS3107 Keys.json')
client = bigquery.Client(credentials=credentials)

@dag(dag_id='sentiment_analysis_transformation', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['IS3107_Project'])
def project():
    
    @task
    def straitstimes_keywords_sentiment_analysis():
        query = f"""
                WITH UnnestedSentiments AS (
                SELECT
                    REGEXP_EXTRACT_ALL(text, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b') AS keywords,
                    sentiments.polarity AS polarity,
                    sentiments.subjectivity AS subjectivity
                FROM
                    is3107-group-10.Dataset.Straitstimes Data with sentiment scores
                )

                SELECT
                keyword,
                COUNT(*) AS frequency,
                AVG(polarity) AS avg_polarity,
                AVG(subjectivity) AS avg_subjectivity
                FROM
                UnnestedSentiments,
                UNNEST(keywords) AS keyword
                WHERE 
                polarity <> 0 -- exclude neutral sentiments
                GROUP BY
                keyword
                ORDER BY
                frequency DESC;
        """

        destination_table = "is3107-group-10.Query_Results.Top Keywords from Straitstimes Associated with Sentiments"
        job_config = bigquery.QueryJobConfig(destination=destination_table)
        client.query(query, job_config=job_config)

    @task
    def reddit_keywords_sentiment_analysis():
        query = f"""
                WITH UnnestedSentiments AS (
                    SELECT
                        REGEXP_EXTRACT_ALL(comments_body, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b') AS comments_keywords,
                        REGEXP_EXTRACT_ALL(text, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b') AS text_keywords,
                        sentiment.polarity AS polarity,
                        sentiment.subjectivity AS subjectivity
                    FROM
                        is3107-group-10.Dataset.Reddit Data with sentiment scores,
                        UNNEST(sentiments) AS sentiment
                )

                SELECT
                keyword,
                COUNT(*) AS frequency,
                AVG(polarity) AS avg_polarity,
                AVG(subjectivity) AS avg_subjectivity
                FROM (
                SELECT keyword, polarity, subjectivity FROM UnnestedSentiments, UNNEST(comments_keywords) AS keyword
                UNION ALL
                SELECT keyword, polarity, subjectivity FROM UnnestedSentiments, UNNEST(text_keywords) AS keyword
                )
                WHERE 
                polarity <> 0 -- exclude neutral sentiments
                GROUP BY
                keyword
                ORDER BY
                frequency DESC;
        """

        destination_table = "is3107-group-10.Query_Results.Top Keywords from Reddit Associated with Sentiments"
        job_config = bigquery.QueryJobConfig(destination=destination_table)
        client.query(query, job_config=job_config)
      
    @task
    def combined_keywords_sentiment_analysis():
        query = f"""
                WITH RedditSentiments AS (
                SELECT
                    REGEXP_EXTRACT_ALL(comments_body, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b') AS comments_keywords,
                    REGEXP_EXTRACT_ALL(text, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b') AS text_keywords,
                    sentiment.polarity AS polarity,
                    sentiment.subjectivity AS subjectivity
                FROM
                    is3107-group-10.Dataset.Reddit Data with sentiment scores,
                    UNNEST(sentiments) AS sentiment
                ),

                StraitsTimesSentiments AS (
                SELECT
                    REGEXP_EXTRACT_ALL(text, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b') AS keywords,
                    sentiments.polarity AS polarity,
                    sentiments.subjectivity AS subjectivity
                FROM
                    is3107-group-10.Dataset.Straitstimes Data with sentiment scores
                )

                SELECT 
                keyword,
                source,
                frequency,
                avg_polarity,
                avg_subjectivity
                FROM (
                SELECT
                keyword,
                'Reddit' AS source,
                COUNT(*) AS frequency,
                AVG(polarity) AS avg_polarity,
                AVG(subjectivity) AS avg_subjectivity
                FROM (
                SELECT keyword, polarity, subjectivity FROM RedditSentiments, UNNEST(comments_keywords) AS keyword
                UNION ALL
                SELECT keyword, polarity, subjectivity FROM RedditSentiments, UNNEST(text_keywords) AS keyword
                )
                WHERE
                polarity <> 0
                GROUP BY
                keyword

                UNION ALL

                SELECT
                keyword,
                'Straits Times' AS source,
                COUNT(*) AS frequency,
                AVG(polarity) AS avg_polarity,
                AVG(subjectivity) AS avg_subjectivity
                FROM
                StraitsTimesSentiments,
                UNNEST(keywords) AS keyword
                WHERE
                polarity <> 0
                GROUP BY
                keyword
                )
                ORDER BY
                frequency DESC
        """

        destination_table = "is3107-group-10.Query_Results.Combined Overall Sentiment Analysis"
        job_config = bigquery.QueryJobConfig(destination=destination_table)
        client.query(query, job_config=job_config)

    @task
    def correlation_analysis_sentiment_analysis():
        query = f"""
                WITH RedditSentiments AS (
                SELECT
                    keyword AS keyword_1,
                    sentiment.polarity AS polarity_1,
                    sentiment.subjectivity AS subjectivity_1
                FROM
                    (
                    SELECT
                        keyword,
                        sentiment
                    FROM
                        is3107-group-10.Dataset.Reddit Data with sentiment scores,
                        UNNEST(sentiments) AS sentiment,
                        UNNEST(REGEXP_EXTRACT_ALL(text, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b')) AS keyword
                    UNION ALL
                    SELECT
                        keyword,
                        sentiment
                    FROM
                        is3107-group-10.Dataset.Reddit Data with sentiment scores,
                        UNNEST(sentiments) AS sentiment,
                        UNNEST(REGEXP_EXTRACT_ALL(comments_body, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b')) AS keyword
                    ) AS all_keywords
                ),

                StraitsTimesSentiments AS (
                SELECT
                    keyword AS keyword_2,
                    sentiments.polarity AS polarity_2,
                    sentiments.subjectivity AS subjectivity_2
                FROM
                    is3107-group-10.Dataset.Straitstimes Data with sentiment scores,
                    UNNEST(REGEXP_EXTRACT_ALL(text, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b')) AS keyword
                )

                SELECT
                keyword_1,
                keyword_2,
                CORR(polarity_1, polarity_2) AS polarity_correlation,
                CORR(subjectivity_1, subjectivity_2) AS subjectivity_correlation
                FROM
                RedditSentiments
                JOIN
                StraitsTimesSentiments
                ON
                RedditSentiments.keyword_1 = StraitsTimesSentiments.keyword_2
                GROUP BY
                keyword_1,
                keyword_2
                ORDER BY
                polarity_correlation DESC, subjectivity_correlation DESC;
        """

        destination_table = "is3107-group-10.Query_Results.Sentiment Correlation Analysis"
        job_config = bigquery.QueryJobConfig(destination=destination_table)
        client.query(query, job_config=job_config)

    @task
    def correlation_analysis_sentiment_analysis():
        query = f"""
                WITH UnnestedSentiments AS (
                SELECT
                    'Reddit' AS data_source,
                    REGEXP_EXTRACT_ALL(comments_body, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b') AS keywords,
                    sentiment.polarity AS polarity,
                    sentiment.subjectivity AS subjectivity
                FROM
                    is3107-group-10.Dataset.Reddit Data with sentiment scores,
                    UNNEST(sentiments) AS sentiment
                UNION ALL
                SELECT
                    'Reddit' AS data_source,
                    REGEXP_EXTRACT_ALL(text, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b') AS keywords,
                    sentiment.polarity AS polarity,
                    sentiment.subjectivity AS subjectivity
                FROM
                    is3107-group-10.Dataset.Reddit Data with sentiment scores,
                    UNNEST(sentiments) AS sentiment
                UNION ALL
                SELECT
                    'Straits Times' AS data_source,
                    REGEXP_EXTRACT_ALL(text, r'\b(government|ministry|budget|GST|CPF|education|healthcare|transport|housing|PAP|elections|ministers)\b') AS keywords,
                    sentiments.polarity AS polarity,
                    sentiments.subjectivity AS subjectivity
                FROM
                    is3107-group-10.Dataset.Straitstimes Data with sentiment scores
                )

                SELECT
                data_source,
                keyword,
                COUNT(*) AS frequency,
                AVG(polarity) AS avg_polarity,
                AVG(subjectivity) AS avg_subjectivity
                FROM (
                SELECT
                    data_source,
                    keyword,
                    polarity,
                    subjectivity
                FROM
                    UnnestedSentiments,
                    UNNEST(keywords) AS keyword
                WHERE
                    polarity <> 0  -- Exclude neutral sentiments
                )
                GROUP BY
                data_source,
                keyword
                ORDER BY
                data_source,
                AVG(polarity) DESC; -- Order by average polarity in descending order
        """

        destination_table = "is3107-group-10.Query_Results.Top Positive and Negative Keywords by Source"
        job_config = bigquery.QueryJobConfig(destination=destination_table)
        client.query(query, job_config=job_config)

    @task
    def combined_overall_sentiment_analysis():
        query = f"""
                WITH UnnestedSentiments AS (
                SELECT
                    'Reddit' AS data_source,
                    sentiment.polarity AS polarity,
                    sentiment.subjectivity AS subjectivity
                FROM
                    is3107-group-10.Dataset.Reddit Data with sentiment scores,
                    UNNEST(sentiments) AS sentiment
                UNION ALL
                SELECT
                    'Straits Times' AS data_source,
                    sentiments.polarity AS polarity,
                    sentiments.subjectivity AS subjectivity
                FROM
                    is3107-group-10.Dataset.Straitstimes Data with sentiment scores
                )

                SELECT
                data_source,
                AVG(polarity) AS avg_polarity,
                AVG(subjectivity) AS avg_subjectivity
                FROM
                UnnestedSentiments
                WHERE 
                polarity <> 0 -- Exclude neutral sentiments
                GROUP BY
                data_source;
        """

        destination_table = "is3107-group-10.Query_Results.Combined Overall Sentiment Analysis"
        job_config = bigquery.QueryJobConfig(destination=destination_table)
        client.query(query, job_config=job_config)
    

    straitstimes_keywords_sentiment_analysis()
    reddit_keywords_sentiment_analysis()
    combined_keywords_sentiment_analysis()
    correlation_analysis_sentiment_analysis()
    correlation_analysis_sentiment_analysis()
    combined_overall_sentiment_analysis()

Project_DAG = project()

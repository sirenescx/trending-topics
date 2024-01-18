import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

PATH = Path(__file__).parent.parent.parent.resolve()
sys.path.append(str(PATH))
from trending_topics.utils import create_parser_instance, read_feeds_config, to_pandas_df, save_to_parquet

dag = DAG(
    dag_id='mnkh_data_fetching_dag',
    schedule_interval='0 * * * *',
    start_date=pendulum.datetime(2024, 1, 16, tz='Europe/Moscow'),
    catchup=False,
    tags=['trending_topics'],
    default_args={
        'owner': 'Maria Manakhova',
        'email': 'manakhova.m@ya.ru',
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=1)
    }
)


def fetch_data():
    parsed_feeds = list()
    for feed in read_feeds_config().values():
        url = feed['url']
        parser = create_parser_instance(feed['parser'])
        parsed_feeds.extend(parser.parse(url))
    return to_pandas_df(parsed_feeds)


def upload_to_hdfs():
    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .appName('trending_topics_write_to_hdfs')
        .getOrCreate()
    )
    save_to_parquet(
        spark=spark,
        data=fetch_data()
    )
    spark.stop()


fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=upload_to_hdfs,
    dag=dag
)

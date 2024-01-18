import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

PATH = Path(__file__).parent.parent.parent.resolve()
sys.path.append(str(PATH))
from trending_topics.utils import save_statistics, read_parquet_from_hdfs, read_parquet
from trending_topics.pipelines.operations.aggregate import AggregationOperation
from trending_topics.pipelines.operations.filter import FilteringOperation
from trending_topics.pipelines.operations.collect import CollectionOperation
from trending_topics.pipelines.operations.n_grams import NGramsComputingOperation
from trending_topics.pipelines.operations.preprocess_text import TextPreprocessingOperation
from trending_topics.pipelines.statistics_computation import ComputeStatisticsPipeline

dag = DAG(
    dag_id='mnkh_statistics_calculation_dag',
    schedule_interval='10 * * * *',
    start_date=pendulum.datetime(2024, 1, 17, tz='Europe/Moscow'),
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


def calculate_statistics():
    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .appName('calculate_statistics')
        .config('spark.num.executors', '16')
        .config('spark.default.parallelism', '16')
        .config('spark.executor.cores', '2')
        .getOrCreate()
    )
    data = read_parquet(spark)
    pipeline = ComputeStatisticsPipeline(
        filtering_op=FilteringOperation(),
        text_preprocessing_op=TextPreprocessingOperation(),
        n_grams_computing_op=NGramsComputingOperation(),
        aggregation_op=AggregationOperation(),
        collection_op=CollectionOperation()
    )
    statistics = pipeline.compute(data)
    save_statistics(statistics)
    spark.stop()


calculate_statistics_task = PythonOperator(
    task_id='calculate_statistics',
    python_callable=calculate_statistics,
    dag=dag
)

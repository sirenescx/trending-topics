import importlib
import json
import time
from pathlib import Path

import pandas as pd

from trending_topics.data.newsletter import Newsletter

CONFIG_PATH: str = 'configs/conf.json'
FEEDS_CONFIG_PATH: str = 'configs/feeds.json'
ROOT_DIRECTORY: Path = Path(__file__).resolve().parent.parent


def get_current_timestamp():
    return int(time.time())


def create_parser_instance(parser_name):
    module_name: str = f'trending_topics.parsers.{parser_name}'
    return getattr(importlib.import_module(module_name), f'{parser_name.title()}Parser')()


def read_feeds_config(base_directory: str = ROOT_DIRECTORY):
    path = Path(base_directory) / FEEDS_CONFIG_PATH if base_directory is not None else FEEDS_CONFIG_PATH
    with open(path, 'r') as _in:
        feeds: dict[str, dict[str, str]] = json.load(_in)['feeds']
    return feeds


def read_config(base_directory: str = ROOT_DIRECTORY):
    path = Path(base_directory) / CONFIG_PATH if base_directory is not None else CONFIG_PATH
    with open(path, 'r') as _in:
        return json.load(_in)


def to_pandas_df(newsletters: list[Newsletter]):
    data = [newsletter.__dict__ for newsletter in newsletters]
    return pd.DataFrame(data=data).astype(Newsletter.get_pandas_types())


def save_to_hdfs(spark, data, schema):
    spark_df = spark.createDataFrame(
        data=data,
        schema=schema
    )
    config = read_config()
    hdfs_path: Path = Path(config['hdfs_path'])
    spark_df.write.mode('append').parquet(str(hdfs_path / str(get_current_timestamp())))


def read_parquet(spark):
    config = read_config()
    hdfs_path: Path = Path(config['hdfs_path'])
    return spark.read.parquet(str(hdfs_path))


def save_statistics(data):
    config = read_config()
    output_path = Path(config['outputs_path'])
    with open(output_path / 'statistics.json', 'w') as _out:
        json.dumps(dict(data.collect()))

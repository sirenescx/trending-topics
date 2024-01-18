from dataclasses import dataclass

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, LongType


@dataclass
class Newsletter:
    source: str
    title: str
    published: int
    summary: str = ''
    tag: str = ''

    @staticmethod
    def get_pandas_types():
        return {
            'source': pd.StringDtype(),
            'title': pd.StringDtype(),
            'published': pd.Int64Dtype(),
            'summary': pd.StringDtype(),
            'tag': pd.StringDtype(),
        }

    @staticmethod
    def get_spark_schema():
        return StructType([
            StructField('source', StringType(), nullable=False),
            StructField('title', StringType(), nullable=False),
            StructField('published', LongType(), nullable=False),
            StructField('summary', StringType(), nullable=False),
            StructField('tag', StringType(), nullable=False)
        ])

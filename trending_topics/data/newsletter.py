from dataclasses import dataclass
from typing import Optional

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, LongType


@dataclass
class Newsletter:
    source: str
    title: str
    published: int
    summary: Optional[str] = None
    content: Optional[str] = None
    full_text: Optional[str] = None
    tag: Optional[str] = None

    @staticmethod
    def get_pandas_types():
        return {
            'source': pd.StringDtype(),
            'title': pd.StringDtype(),
            'published': pd.Int64Dtype(),
            'summary': pd.StringDtype(),
            'content': pd.StringDtype(),
            'full_text': pd.StringDtype(),
            'tag': pd.StringDtype(),
        }

    @staticmethod
    def get_spark_schema():
        return StructType([
            StructField('source', StringType(), nullable=False),
            StructField('title', StringType(), nullable=False),
            StructField('published', LongType(), nullable=False),
            StructField('summary', StringType(), nullable=True),
            StructField('content', StringType(), nullable=True),
            StructField('full_text', StringType(), nullable=True),
            StructField('tag', StringType(), nullable=True)
        ])
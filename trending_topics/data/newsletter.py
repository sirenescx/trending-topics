from dataclasses import dataclass
from typing import Optional

import pandas as pd


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

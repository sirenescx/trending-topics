from pyspark.sql.functions import col, lit

from trending_topics.pipelines.utils import phrases_blacklist
from trending_topics.utils import get_current_timestamp


class FilteringOperation:
    def filter_by_timestamp(self, data, interval=86400 * 7):
        return (
            data
            .withColumn('published', col('published').cast('int'))
            .filter(
                lit(get_current_timestamp()) - col('published') <= interval
            )
            .distinct()
        )

    def filter_by_blacklist(self, data):
        return (
            data
            .filter(~col('aggregated_n_grams').isin(phrases_blacklist))
        )

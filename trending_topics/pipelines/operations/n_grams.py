from pyspark.sql.functions import col

from trending_topics.pipelines.utils import get_n_grams


class NGramsComputingOperation:
    def compute_n_grams(self, data):
        return (
            data
            .select(
                get_n_grams(col('title')).alias('title_n_grams'),
                get_n_grams(col('summary')).alias('summary_n_grams'),
                get_n_grams(col('tag')).alias('tag_n_grams'),
            )
        )

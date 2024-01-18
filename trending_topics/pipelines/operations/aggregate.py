from pyspark.sql.functions import explode, col


class AggregationOperation:
    def aggregate(self, data):
        return (
            data
            .select(
                explode(col('title_n_grams')).alias('n_gram')
            ).union(
                data
                .select(
                    explode(col('summary_n_grams')).alias('n_gram'))
            ).union(
                data
                .select(
                    explode(col('tag_n_grams')).alias('n_gram'))
            )
        )

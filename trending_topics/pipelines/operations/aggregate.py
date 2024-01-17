from pyspark.sql.functions import explode, flatten, array, col


class AggregationOperation:
    def aggregate(self, data):
        return (
            data
            .select(
                explode(
                    flatten(
                        array(
                            col('summary_n_grams'),
                            col('content_n_grams'),
                            col('full_text_n_grams'),
                            col('tag_n_grams')
                        )
                    )
                ).alias('aggregated_n_grams')
            )
        )

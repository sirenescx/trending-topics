from pyspark.sql.functions import col


class CollectionOperation:
    def collect(self, data, limit=1000):
        n_gram_frequency = (
            data
            .groupBy('n_gram')
            .count()
        )
        n_gram_frequency.cache()
        return (
            n_gram_frequency
            .orderBy(col('count').desc())
            .limit(limit)
            .cache()
            .collect()
        )

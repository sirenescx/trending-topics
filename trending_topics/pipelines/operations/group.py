from pyspark.sql.functions import col


class GroupingOperation:
    def group(self, data, limit=500):
        return (
            data
            .groupBy('aggregated_n_grams')
            .count()
            .withColumnRenamed('count', 'frequency')
            .orderBy(col('frequency').desc())
            .limit(limit)
        )

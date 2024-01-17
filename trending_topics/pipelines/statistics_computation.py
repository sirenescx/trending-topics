from trending_topics.pipelines.operations.aggregate import AggregationOperation
from trending_topics.pipelines.operations.filter import FilteringOperation
from trending_topics.pipelines.operations.group import GroupingOperation
from trending_topics.pipelines.operations.n_grams import NGramsComputingOperation
from trending_topics.pipelines.operations.preprocess_text import TextPreprocessingOperation


class ComputeStatisticsPipeline:
    def __init__(
            self,
            filtering_op: FilteringOperation,
            text_preprocessing_op: TextPreprocessingOperation,
            n_grams_computing_op: NGramsComputingOperation,
            aggregation_op: AggregationOperation,
            grouping_op: GroupingOperation
    ):
        self.filtering_op = filtering_op
        self.text_preprocessing_op = text_preprocessing_op
        self.n_grams_computing_op = n_grams_computing_op
        self.aggregation_op = aggregation_op
        self.grouping_op = grouping_op

    def compute(self, data):
        data_filtered_by_timestamp = self.filtering_op.filter_by_timestamp(data)
        data_with_preprocessed_text = self.text_preprocessing_op.preprocess_text(data_filtered_by_timestamp)
        data_with_n_grams = self.n_grams_computing_op.compute_n_grams(data_with_preprocessed_text)
        data_aggregated = self.aggregation_op.aggregate(data_with_n_grams)
        data_aggregated_filtered_by_blacklist = self.filtering_op.filter_by_blacklist(data_aggregated)
        data_grouped = self.grouping_op.group(data_aggregated_filtered_by_blacklist)
        return data_grouped

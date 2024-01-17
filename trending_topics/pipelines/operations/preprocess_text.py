from trending_topics.pipelines.utils import remove_punctuation_and_lower


class TextPreprocessingOperation:
    def preprocess_text(self, data):
        return (
            data
            .select(
                remove_punctuation_and_lower('summary').alias('summary'),
                remove_punctuation_and_lower('content').alias('content'),
                remove_punctuation_and_lower('full_text').alias('full_text'),
                remove_punctuation_and_lower('tag').alias('tag')
            )
        )

import string

import nltk
from nltk.corpus import stopwords
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType

nltk.download('stopwords')
stop_words = set(stopwords.words('english')).union(set(stopwords.words('russian')))
phrases_blacklist = [
    '', 'said', 'это', 'который', 'год', 'year', 'city', 'new',
    'according', 'according to', 'свой', 'также', 'заявить',
    'новый', 'would', 'also', 'со ссылкой', 'ссылка',
    'ссылкой на', 'со ссылкой на', 'ещё'
]


@udf(StringType())
def remove_punctuation_and_lower(text):
    if text is None:
        return text
    punctuation = string.punctuation.join(['—', '«', '»', '-', '“'])
    translator = str.maketrans('', '', punctuation)
    text_without_punctuation = text.translate(translator)
    return text_without_punctuation.lower()


@udf(ArrayType(StringType()))
def get_n_grams(text):
    def filter_n_grams(n_grams):
        return not all(token in stop_words for token in n_grams)

    def join_n_grams(n_grams):
        return ' '.join(n_grams)

    def get_unigrams(text):
        def to_normal_form(word):
            import pymorphy2

            morph_analyzer = pymorphy2.MorphAnalyzer()
            return morph_analyzer.parse(word)[0].normal_form

        if text is None:
            return []

        normal_forms = list(map(lambda word: to_normal_form(word), text.split(' ')))
        return list(filter(lambda normal_form: normal_form not in stop_words, normal_forms))

    def get_bigrams(text):
        if text is None:
            return []

        tokens = text.split(' ')
        bigrams = zip(tokens, tokens[1:])
        return list(map(join_n_grams, filter(filter_n_grams, bigrams)))

    def get_trigrams(text):
        if text is None:
            return []

        tokens = text.split(' ')
        trigrams = zip(tokens, tokens[1:], tokens[2:])
        return list(map(join_n_grams, filter(filter_n_grams, trigrams)))

    n_grams = get_unigrams(text)
    n_grams.extend(get_bigrams(text))
    n_grams.extend(get_trigrams(text))

    return n_grams


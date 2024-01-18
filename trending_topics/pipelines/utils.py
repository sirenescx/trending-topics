import string
from collections import Counter
from dataclasses import dataclass

import nltk
from nltk.corpus import stopwords
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, LongType, MapType

nltk.download('stopwords')
stop_words = set(stopwords.words('english')).union(set(stopwords.words('russian')))
phrases_blacklist = [
    '', 'said', 'это', 'который', 'год', 'year', 'city', 'new',
    'according', 'according to', 'свой', 'также', 'заявить',
    'новый', 'would', 'also', 'со ссылкой', 'ссылка',
    'ссылкой на', 'со ссылкой на', 'ещё', 'сообщить'
]


@udf(StringType())
def remove_punctuation_and_lower(text):
    def compress_multiple_whitespaces(text):
        import re
        return re.sub(r'\s\s+', ' ', text)

    if text is None:
        return text
    punctuation = string.punctuation.join(['—', '«', '»', '-', '“'])
    translator = str.maketrans('', '', punctuation)
    text_without_punctuation = text.translate(translator)
    text_with_compressed_whitespaces = compress_multiple_whitespaces(text_without_punctuation)
    return text_with_compressed_whitespaces.lower().strip()


@udf(ArrayType(StringType()))
def get_n_grams(text):
    def filter_n_grams(n_grams):
        return not all(token in stop_words for token in n_grams)

    def join_n_grams(n_grams):
        return ' '.join(n_grams)

    def to_normal_form(word):
        import pymorphy2

        morph_analyzer = pymorphy2.MorphAnalyzer()
        return morph_analyzer.parse(word)[0].normal_form

    if text is None or text == '':
        return []

    tokens: list[str] = text.split(' ')
    n_grams: list[str] = list()

    unigrams = list(map(lambda word: to_normal_form(word), tokens))
    n_grams.extend(list(filter(lambda normal_form: normal_form not in stop_words, unigrams)))

    bigrams = zip(tokens, tokens[1:])
    n_grams.extend(list(map(join_n_grams, filter(filter_n_grams, bigrams))))

    trigrams = zip(tokens, tokens[1:], tokens[2:])
    n_grams.extend(list(map(join_n_grams, filter(filter_n_grams, trigrams))))

    return n_grams

@udf(MapType(StringType(), LongType()))
def count_frequency(title_n_grams, summary_n_grams, tag_n_grams):
    n_grams_frequency: dict[str, int] = dict()
    n_grams_frequency.update(Counter(title_n_grams))
    n_grams_frequency.update(Counter(summary_n_grams))
    n_grams_frequency.update(Counter(tag_n_grams))

    return n_grams_frequency

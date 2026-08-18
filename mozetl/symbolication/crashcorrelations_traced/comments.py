import re

from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.sql import functions

from porter2 import stem


def get_top_words(dataset, signatures):
    # TODO: Use stemmers for the languages supported by http://www.nltk.org/api/nltk.stem.html#nltk.stem.snowball.SnowballStemmer
    # Or translate comments in other languages using the free Microsoft Translate API.
    sentenceData = dataset.filter(dataset['user_comments'].isNotNull() & (dataset['useragent_locale'].isNull() | (functions.instr(dataset['useragent_locale'], 'en') == 1)))

    if sentenceData.rdd.isEmpty():
        return dict()

    # Tokenize comments.
    tokenizer = Tokenizer(inputCol='user_comments', outputCol='words')
    wordsData = tokenizer.transform(sentenceData)

    # Remove duplicate words from comments.
    wordsData = wordsData.rdd.map(lambda p: (p['signature'], list(set(p['words'])))).reduceByKey(lambda x, y: x + y).toDF(['signature', 'words'])

    if wordsData.rdd.isEmpty():
        print("[WARNING]: wordsData is empty, sentenceData wasn't.")
        return dict()

    # Clean comment words by removing puntuaction and stemming.
    def clean_word(w):
        return re.sub('\,|\.|\;|\:|\;|\?|\!|\[|\]|\}|\{|\/|\\\\', '', stem(w.lower()))

    wordsData = wordsData.rdd.map(lambda p: (p['signature'], [clean_word(w) for w in p['words']])).toDF(['signature', 'words'])

    # XXX: Useless with TF-IDF?
    remover = StopWordsRemover(inputCol='words', outputCol='filtered')
    cleanWordsData = remover.transform(wordsData)

    cv = CountVectorizer(inputCol='filtered', outputCol='features')
    model = cv.fit(cleanWordsData)
    featurizedData = model.transform(cleanWordsData)

    idf = IDF(inputCol='features', outputCol='tfidf_features')
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    bests_per_doc = rescaledData.filter(rescaledData.signature.isin(signatures)).rdd.map(lambda p: (p['signature'], sorted(zip(p['tfidf_features'].indices, p['tfidf_features'].values), key=lambda i: i[1], reverse=True)[:10])).collect()

    return dict([(signature, [model.vocabulary[best] for best, val in bests]) for signature, bests in bests_per_doc])

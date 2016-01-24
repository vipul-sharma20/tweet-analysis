from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import EnglishStemmer
import nltk
from nltk import word_tokenize
import urllib2
import json

URL_SENTIMENT140 = "http://www.sentiment140.com/api/bulkClassifyJson"

def remove_stopwords(sentence):
    """
    Removes stopwords from the sentence
    :param sentence: (str) sentence
    :returns: cleaned sentence without any stopwords
    """
    sw = set(stopwords.words('english'))
    cleaned = []
    words = get_words(sentence)
    sentence = ' '.join([c for c in words if c not in sw])
    return sentence

def get_words(sentence):
    """
    Extracts words/tokens from a sentence
    :param sentence: (str) sentence
    :returns: list of tokens
    """
    words = word_tokenize(sentence)
    return words

def stem_word(word):
    """
    Stem words
    :param word: (str) text word
    :returns: stemmed word
    """
    stemmer = EnglishStemmer()
    return stemmer.stem(word)

def sentiment(data):
    """
    sentiment analysis using sentiment140
    :param data: (list) tweets
    :returns: tweets tagged with polarity
    """
    data = json.dumps(data)
    response = urllib2.urlopen(URL_SENTIMENT140, data)
    json_response = json.loads(response.read())
    return json_response

def intent(data):
    """
    define intent of the tweet
    :param data: (str) cleaned up tweet
    :returns: intent
    """
    accept = ['VB', 'VBG', 'VBD', 'VBN', 'VBP', 'VBZ']
    tweet = word_tokenize(data)
    tagged = nltk.pos_tag(tweet)
    result = []
    for tags in tagged:
        if tags[1] in accept:
            result.append(stem_word(tags[0]))
    return result

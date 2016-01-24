"""
    '$ bin/spark-submit path-to-tweet.py'
"""

import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import twitter
import dateutil.parser
import json
import util
from util import *

# Connecting Streaming Twitter with Streaming Spark via Queue
class Tweet(dict):
    def __init__(self, tweet_in):
        super(Tweet, self).__init__(self)
        if tweet_in and 'delete' not in tweet_in:
            self['timestamp'] = dateutil.parser.parse(tweet_in[u'created_at']
                                ).replace(tzinfo=None).isoformat()
            #self['text'] = tweet_in['text'].encode('utf-8')
            self['text'] = tweet_in['text']
            #self['hashtags'] = [x['text'].encode('utf-8') for x in tweet_in['entities']['hashtags']]
            self['hashtags'] = [x['text'] for x in tweet_in['entities']['hashtags']]
            self['geo'] = tweet_in['geo']['coordinates'] if tweet_in['geo'] else None
            self['id'] = tweet_in['id']
            #self['screen_name'] = tweet_in['user']['screen_name'].encode('utf-8')
            self['screen_name'] = tweet_in['user']['screen_name']
            self['user_id'] = tweet_in['user']['id']

def connect_twitter():
    """
    Twitter API config
    """
    twitter_stream = twitter.TwitterStream(auth=twitter.OAuth(
        token = add_token_here,
        token_secret = add_token_secret_here,
        consumer_key = add_consumer_key_here),
        consumer_secret = add_consumer_secret_here))
    return twitter_stream

def get_next_tweet(twitter_stream):
    """
    Get tweets
    """
    stream = twitter_stream.statuses.sample()
    tweet_in = None
    tweet_parsed = []
    for t in stream:
        u = t.get('user')
        if t != None and not 'delete' in t and u:
            if u.get('lang') == 'en':
                tweet_in = t
                tweet_parsed.append(json.dumps(Tweet(tweet_in)))
    return tweet_parsed

def process_rdd_queue(twitter_stream):
    rddQueue = [ssc.sparkContext.parallelize(get_next_tweet(twitter_stream))]

    lines = ssc.queueStream(rddQueue)
    data = rddQueue[0].collect()

    lines.pprint()
    process(data)

def process(data):
    tweets = []
    for d in data:
        temp_data = dict()
        t = json.loads(d)
        temp_data['text'] = t['text']
        temp_data['id'] = t['id']
        tweets.append(temp_data)

    result = {"data": tweets}
    response = sentiment(result)

    for d in data:
        t = json.loads(d)
        refined = remove_stopwords(t['text'])
        intention = intent(refined)

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingQueueStream")
    ssc = StreamingContext(sc, 1)

    # Create twitter_stream instance
    twitter_stream = connect_twitter()
    # Get RDD queue of the streams json or parsed
    process_rdd_queue(twitter_stream)

    ssc.start()
    time.sleep(2)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)

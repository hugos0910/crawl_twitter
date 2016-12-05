#!/usr/bin/env python 
# crawl.py config.json

import time, datetime
import json
import MySQLdb
import sys
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

def xpath(value, path):
    for name in path.split(): 
        value = value.get(name) 
        if not value: return None
    return value

class MySQLListener(StreamListener):
    def __init__(self, config):
        super(StreamListener, self).__init__()
        self.connection = MySQLdb.connect(host=config['mysql_host'], 
                         user=config['mysql_user'], 
                         passwd=config['mysql_password'],
                         db=config['mysql_database'])
        self.cursor = self.connection.cursor()

    def on_data(self, data):
        tweet = json.loads(data)
        coordinates = xpath(tweet, "coordinates coordinates")
        lat = str(coordinates[1]) if coordinates else "null"
        lng = str(coordinates[0]) if coordinates else "null"
        city = xpath(tweet, "place full_name") if xpath(tweet, "place place_type") == "city" else "null"
        country = xpath(tweet, "place country") or "null"
        name = xpath(tweet, "user name")
        created_at = int(time.mktime(datetime.datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S +0000 %Y").timetuple()))
        query = "INSERT INTO tweets VALUES (default, %s, %s, %d, %s, %s, %d, \"%s\", \"%s\", %d)" % (lng, lat, tweet.get("favrotie_count", 0), city, country, tweet.get("retweet_count", 0), tweet.get("text"), name, created_at)
        self.cursor.execute(query)
        self.connection.commit()

if __name__ == '__main__':
    config = json.load(open(sys.argv[1]))
    
    listener = MySQLListener(config)
    auth = OAuthHandler(config['consumer_key'], consumer_key['consumer_secret'])
    auth.set_access_token(consumer_key['access_token'], consumer_key['access_token_secret'])
    stream = Stream(auth, listener)
    stream.filter(track=['python', 'javascript', 'ruby'], languages=["en"])

#!/usr/bin/env python 
# crawl.py config.json

import time, datetime
import json
import MySQLdb
import sys
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

class MySQLListener(StreamListener):
  def __init__(self, config):
    super(StreamListener, self).__init__()
    self.connection = MySQLdb.connect(host=config['mysql_host'], 
                                      user=config['mysql_user'], 
                                      passwd=config['mysql_password'],
                                      db=config['mysql_database'])
    self.cursor = self.connection.cursor()

  def on_data(self, data):
    def xpath(value, path):
      for name in path.split(): 
        value = value.get(name) 
        if not value: return None
      return value

    def sanitize_string(s):
      return "\"%s\"" % s.encode('utf8') if s else "null"
      
    tweet = json.loads(data)
    coordinates = xpath(tweet, "coordinates coordinates")
    lat = str(coordinates[1]) if coordinates else None
    lng = str(coordinates[0]) if coordinates else None
    city = xpath(tweet, "place full_name") if xpath(tweet, "place place_type") == "city" else None
    country = xpath(tweet, "place country")
    name = xpath(tweet, "user name")
    created_at = int(time.mktime(datetime.datetime
                   .strptime(tweet["created_at"], "%a %b %d %H:%M:%S +0000 %Y").timetuple()))
    query = ("INSERT INTO tweets VALUES (default, %d, %s, %s, %s, %s, %s, %s)"
               % (created_at,
                  lat or "null",
                  lng or "null",
                  sanitize_string(city),
                  sanitize_string(country),
                  sanitize_string(name),
                  sanitize_string(tweet["text"])))
    self.cursor.execute(query)
    self.connection.commit()

if __name__ == '__main__':
  config = json.load(open(sys.argv[1]))
  auth = OAuthHandler(config['consumer_key'], config['consumer_secret'])
  auth.set_access_token(config['access_token'], config['access_token_secret'])
  
  stream = Stream(auth, MySQLListener(config))
  stream.filter(track=['swift', 'golang'], languages=["en"])

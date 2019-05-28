#!/usr/bin/python3

import sys, os, getopt, argparse, logging, logging.handlers, traceback, re, subprocess
import tweepy, json

# Authentication details. To obtain these visit dev.twitter.com
G_consumer_key = 'Uuco488uOvdCCbIpemP9nwdN8'
G_consumer_secret = 'GuNz81y2jtkLncOQCOrrmHqwBlAjLD3SAhO735cWAzBvt3fm96'
G_access_token = '1133252394383335424-KGDg30Qr1am9dIs4yUmyMtW5XImt3n'
G_access_token_secret = 'Z8kGB9EBjJN4WqxZv91beRdlVYC8J8PBxODrZRnJLkfO8'
# keywords
G_track = ['New York','weather','aqi','air quality']
# San Francisco Bay area, New York, LA and Chicago
SF_AREA = [-123.1512,37.0771,-121.3165,38.5396]
NY_AREA = [-74.255735,40.496044,-73.700272,40.915256]
LA_AREA = [-118.6682,33.7037,-118.1553,34.3373]
CHICAGO = [-87.940267,41.644335,-87.524044,42.023131]
G_locations = [-123.1512,37.0771,-121.3165,38.5396,-74.255735,40.496044,-73.700272,40.915256,-118.6682,33.7037,-118.1553,34.3373,-87.940267,41.644335,-87.524044,42.023131]

# 判断语句包含 关键词
def str_contain_tracks(str):
    for word in G_track:
        if word in str:
            return True
    return False

## This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):
    def on_data(self, data):
        # print to strout withput any filter to decoding
        # print ('%s' % json.dumps(json.loads(data)))
        # str(json.load(data.strip())).replace("{u'","{'").replace(" u'"," '")
        tmp = json.loads(data)
        if tmp['lang']=="en" and str_contain_tracks(tmp['text']):
            f1=open('/Users/Kai/Workspace/AirQualitySensor/Collector/log/twitter.log','a')
            f1.write(data)
            f1.close()
            print (data)
        else :
            f2=open('/Users/Kai/Workspace/AirQualitySensor/Collector/log/twitter_useless.log','a')
            f2.write(data)
            f2.close()
        # print (json.dumps(data))
        return True

    def on_error(self, status):
        print(status)


## ____________main_________________
def main(argv):
    # accessing global variable
    global G_consumer_key, G_consumer_secret, G_access_token, G_access_token_secret, G_track

    # twitter configs
    listener = StdOutListener()
    auth = tweepy.OAuthHandler(G_consumer_key, G_consumer_secret)
    auth.set_access_token(G_access_token, G_access_token_secret)

    stream = tweepy.Stream(auth, listener)
    # stream.filter(locations=NY_AREA,languages=["en"],track=G_track)
    stream.filter(locations=G_locations)

## callin main
if __name__ == '__main__':
    main(sys.argv)


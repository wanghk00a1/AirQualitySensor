#!/usr/bin/python3

import sys, os, getopt, argparse, logging, logging.handlers, traceback, re, subprocess
import tweepy, json

# Authentication details. To obtain these visit dev.twitter.com
G_consumer_key = 'aZVfiXFSFYmRCbabANPG9fONQ'
G_consumer_secret = 'S1oqGNfnrdlw24TBd9TvcHqdtglhainhwETC7loDuGlFY22ICk'
G_access_token = '1133252394383335424-GiofM2WUeEuesmuZqfo03P70pPl7X6'
G_access_token_secret = 'DMTSobTusIrGi3rlnsRqKuKMnUtcsijsa7FClSzVMSp6t'
# keywords
G_track = ['weather','aqi','air quality','nice','bad','health']
# San Francisco Bay area, New York, LA and Chicago
SF_AREA = [-123.1512,37.0771,-121.3165,38.5396]
NY_AREA = [-74.255735,40.496044,-73.700272,40.915256]
LA_AREA = [-118.6682,33.7037,-118.1553,34.3373]
CHICAGO = [-87.940267,41.644335,-87.524044,42.023131]
LONDON = [-0.5104,51.2868,0.334,51.6919]
# london + new york
G_locations = [-0.5104,51.2868,0.334,51.6919,-74.255735,40.496044,-73.700272,40.915256]

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
        print (data)
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


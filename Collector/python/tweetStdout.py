#!/usr/bin/python3

import sys, os, getopt, argparse, logging, logging.handlers, traceback, re, subprocess
import tweepy, json

# Authentication details. To obtain these visit dev.twitter.com
G_consumer_key = 'pH8zAmloqT0xBwit303LI0zPd'
G_consumer_secret = 'h1NfmL8t3Ry60eranoz61PYWXNir9539QyzQ0i4L2jqaU0IQDC'
G_access_token = '4244469072-FxT513aKjBWrSZipMYzhMdlN6AYha77d90MV3Hh'
G_access_token_secret = 'ks69jGoDCEVeKarwBqztsoWt0xdPIamllVKq9MgGu3NMi'
# keywords
G_track = ['Oscar','football','Avengers']

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
    stream.filter(track=G_track)


## callin main
if __name__ == '__main__':
    main(sys.argv)


from secrets import access_token, access_token_secret, \
    api_key, api_key_secret

import tweepy
import json


class Listener(tweepy.StreamListener):

    def on_data(self, data):
        data_dict = json.loads(data)
        print(data_dict['text'])

    def on_error(self, status_code):
        print(status_code)

auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

miner = Listener()
stream = tweepy.Stream(auth=api.auth, listener=miner)

stream.filter(track=['league of legends'])


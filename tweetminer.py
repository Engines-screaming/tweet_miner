from secrets import access_token, access_token_secret, \
    api_key, api_key_secret

import tweepy
import json
import sqlite3
from textblob import TextBlob


database_name = 'female_rappers'
tags = ['megan thee stallion', 'nicki minaj', 'cardib']


class MineCart(tweepy.StreamListener):
    '''Initialize with a list of category labels to put in the database'''

    def __init__(self, categories):
        self.categories = categories

    def on_data(self, data):
        data_dict = json.loads(data)
        
        tb = TextBlob(data_dict['text'])

        # establish db and insert 
        conn = sqlite3.connect(f'{database_name}.db')
        c = conn.cursor()

        # conditional insert based on filter
        for category in self.categories:
            if category in data_dict['text']:
                values = (data_dict['id_str'], category, data_dict['text'], tb.sentiment.polarity)
                c.execute(f'''INSERT INTO tweets VALUES {values}''')
                conn.commit()
                print('tweet inserted to db')

        conn.close()

    def on_error(self, status_code):
        print(status_code)


def mine_tweets():
    ''' Main function to mine tweets and store in database'''
    
    # authentication and setup listener
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    miner = MineCart(tags)

    # start database
    conn = sqlite3.connect(f'{database_name}.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tweets
                    (tweet_id TEXT PRIMARY KEY, carrier TEXT, tweet TEXT, sentiment_score REAL)''')
    conn.commit()
    conn.close()
    
    # start a stream for each category
    stream = tweepy.Stream(auth=api.auth, listener=miner)
    stream.filter(track=tags)


if __name__ == '__main__':
    mine_tweets()

from secrets import access_token, access_token_secret, \
    api_key, api_key_secret

import tweepy
import json
import sqlite3
from textblob import TextBlob


database_name = 'food'
tags = ['hotdog', 'pizza', 'burger', 'taco']


class MineCart(tweepy.StreamListener):
    '''Initialize with a list of category labels to put in the database'''

    def __init__(self, categories, verbose=False):
        self.categories = categories
        self.verbose = verbose
        self.tweets_stored = 0

    def clean_tweet(self, tweet):
        '''method to clean the tweet before inserting to db'''

        removed_quote = tweet.replace("'", "")
        cleaned = removed_quote.lower()

        return cleaned

    def on_data(self, data):
        data_dict = json.loads(data)

        tb = TextBlob(data_dict['text'])

        # establish db and insert 
        conn = sqlite3.connect(f'data/{database_name}.db')
        c = conn.cursor()

        # conditional insert based on filter
        cleaned_text = self.clean_tweet(data_dict['text']) # clean text for category matching
        for category in self.categories:
            if category in cleaned_text:
                values = (category, cleaned_text, tb.sentiment.polarity)

                try:
                    c.execute(f'''INSERT INTO tweets VALUES {values};''')
                except Exception as e:
                    print(e)
                    print(values)
                    raise Exception

                conn.commit()

                if self.verbose:
                    print(data_dict['text'])

                self.tweets_stored += 1
                print(f'{self.tweets_stored} tweet(s) stored in db')

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
    conn = sqlite3.connect(f'data/{database_name}.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tweets
                    (category TEXT, tweet TEXT, sentiment_score REAL)''')
    conn.commit()
    conn.close()
    
    # start a stream for each category
    stream = tweepy.Stream(auth=api.auth, listener=miner)
    stream.filter(track=tags)


if __name__ == '__main__':
    mine_tweets()

from secrets import access_token, access_token_secret, \
    api_key, api_key_secret

import tweepy
import json
import sqlite3
from textblob import TextBlob


class Listener(tweepy.StreamListener):

    def on_data(self, data):
        data_dict = json.loads(data)
        
        tb = TextBlob(data_dict['text'])
        values = (data_dict['id_str'], data_dict['text'], tb.sentiment.polarity)

        # establish db and insert 
        conn = sqlite3.connect(f'att_tweets.db')
        c = conn.cursor()
        c.execute(f'''INSERT INTO tweets VALUES {values}''')
        conn.commit()
        conn.close()

        print('tweet inserted to db')

    def on_error(self, status_code):
        print(status_code)


def mine_tweets():
    ''' Main function to mine tweets and store in database'''
    
    # authentication and setup listener
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    miner = Listener()

    # start database
    conn = sqlite3.connect('att_tweets.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tweets
                    (tweet_id TEXT PRIMARY KEY , tweet TEXT, sentiment_score REAL)''')
    conn.commit()
    conn.close()
    
    # start listener
    stream = tweepy.Stream(auth=api.auth, listener=miner)
    stream.filter(track=['@ATT'])


if __name__ == '__main__':
    mine_tweets()

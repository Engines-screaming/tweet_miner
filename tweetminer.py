from secrets import access_token, access_token_secret, \
    api_key, api_key_secret

import tweepy
import json
import sqlite3
from textblob import TextBlob


class MineCart(tweepy.StreamListener):

    def on_data(self, data):
        data_dict = json.loads(data)
        
        tb = TextBlob(data_dict['text'])

        # establish db and insert 
        conn = sqlite3.connect(f'wireless_tweets.db')
        c = conn.cursor()

        # conditional insert based on filter
        if 'ATT' in data_dict['text']:
            category = 'ATT'
            values = (data_dict['id_str'], category, data_dict['text'], tb.sentiment.polarity)
            c.execute(f'''INSERT INTO tweets VALUES {values}''')
            conn.commit()
            print('tweet inserted to db')

        if 'Verizon' in data_dict['text']:
            category = 'VZN'
            values = (data_dict['id_str'], category, data_dict['text'], tb.sentiment.polarity)
            c.execute(f'''INSERT INTO tweets VALUES {values}''')
            conn.commit()
            print('tweet inserted to db')
            
        if 'Tmobile' in data_dict['text']:
            category = 'TMO'
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
    miner = MineCart()

    # start database
    conn = sqlite3.connect('wireless_tweets.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tweets
                    (tweet_id TEXT PRIMARY KEY, carrier TEXT, tweet TEXT, sentiment_score REAL)''')
    conn.commit()
    conn.close()
    
    # att stream
    att_stream = tweepy.Stream(auth=api.auth, listener=miner)
    att_stream.filter(track=['ATT'])

    # verizon stream
    vzn_stream = tweepy.Stream(auth=api.auth, listener=miner)
    vzn_stream.filter(track=['Verizon'])

    # tmo stream
    tmo_stream = tweepy.Stream(auth=api.auth, listener=miner)
    tmo_stream.filter(track=['Tmobile'])

if __name__ == '__main__':
    mine_tweets()

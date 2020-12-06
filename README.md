# tweet_miner
collects tweets and places them into sqlite

# Setting up:
```
conda create --name tweetminer python=3
conda activate
pip install -r requirements.txt
```

Next create a file called "secrets.py" where you can store all the api keys for authentication. Fill in the 
values with your keys.
```python
# secrets.py
access_token = 'insert value here'
access_token_secret = 'insert value here'
api_key = 'insert value here'
api_key_secret = 'insert value here'
```
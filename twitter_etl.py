import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import os
from dotenv import load_dotenv

load_dotenv()

access_key = os.getenv("API_KEY")
access_secret = os.getenv("API_KEY_SECRET")
consumer_key = os.getenv("ACCESS_TOKEN")
consumer_secret = os.getenv("ACCESS_TOKEN_SECRET")

auth = tweepy.OAuthHandler(access_key, access_secret)
auth.set_access_token(consumer_key, consumer_secret)

api = tweepy.API(auth)

def run_twitter_etl():

    tweets = api.user_timeline(screen_name='@frankiemacd',
                            count=200,
                            include_rts = False,
                            tweet_mode='extended'
                            )

    tweet_list = []
    for tweet in tweets:

        refined_tweet = {
            'user': tweet.user.screen_name,
            'text': tweet._json["full_text"],
            'favorite_count': tweet.favorite_count,
            'retweet_count': tweet.retweet_count,
            'created_at': tweet.created_at
        }

        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://simbamon-twitter-bucket/franky_tweet_data.csv")
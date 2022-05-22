import service_redis as rs
import streamlit as st
import pandas as pd
import time
import app_constants as appc


def load_tweet_data():
    return pd.DataFrame.from_dict(redisStore.get_tweets())

def load_hashtag_data():
    return pd.DataFrame.from_dict(redisStore.get_hashtags())

def load_user_data(cache_key):
    return pd.DataFrame.from_dict(redisStore.get_user_metrics(cache_key))


def update_user_metrics(a,b,c,d):
    user_key = None
    if user_metric == 'Tweets':
        user_key = appc.RC_USER_TWEET_COUNT
    elif user_metric == 'ReTweets':
        user_key = appc.RC_USER_RETWEET_COUNT
    elif user_metric == 'Follower-Counts':
        user_key = appc.RC_USER_FOLLOWER_COUNT 
    elif user_metric == 'Friends-Counts':
        user_key = appc.RC_USER_FRIENDS_COUNT
    elif user_metric == 'Favourite-Counts':
        user_key = appc.RC_USER_FAV_COUNT 
    elif user_metric == 'Status-Counts':
        user_key = appc.RC_USER_STATUS_COUNT                                 
    else:
        user_key = appc.RC_USER_FOLLOWER_COUNT
    return user_key


redisStore = rs.RedisService()

st.title('Twitter Caching Streaming Data Demo..')

st.subheader('Latest ' + str(appc.RC_MAX_TWEET_COUNT) + ' Tweets..')
table_latest_tweets = st.empty()

st.subheader(str(appc.RC_MAX_HASHTAG_UI_COUNT) + ' Most Popular Hashtags..')
table_latest_hashtags = st.empty()

st.subheader(str(appc.RC_MAX_USER_UI_COUNT) + ' Most Popular User Metrics..')
user_metric = st.radio(
     "Select a user metric to view by relevance",
     ('Tweets', 'ReTweets', 'Follower-Counts', 'Friends-Counts', 'Favourite-Counts', 'Status-Counts'),
     on_change = update_user_metrics, args = ('abcd'))
table_latest_users = st.empty()  
table_latest_users.dataframe(load_user_data(update_user_metrics(None,None,None,None)))    
   


while True:
    table_latest_tweets.dataframe(load_tweet_data())
    table_latest_hashtags.dataframe(load_hashtag_data())
    # update_user_metrics()   
    time.sleep(2)

import streamlit as st 
# pprint library is used to make the output look more pretty
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pandas as pd
import time
import service_searchapp
import data_model as dm
import service_mongo
import sys
sys.setrecursionlimit(10000)
import time

#This part of code creates a sidebar and even gives it a heading 
st.sidebar.header('User Input Parameters')
Page=st.sidebar.selectbox('Select Search Method:',['Home Page', 'User Name', 'Text', 'Hashtag', 'Metrics'])

if(Page == 'Home Page'):
    st.write("""
    # Search Engine for Tweets

    This app finds Tweets and it's information based on type of search made by user.
    """)


if(Page == 'User Name'):
    st.write("""
    # Search Engine for Tweets

    This app finds Tweets and it's information based on type of search made by user.
    """)
    nameinput = st.text_input("Enter the User Name")
    date1_input = st.date_input("Enter Start Date")
    date2_input = st.date_input("Enter End Date")
    sr = dm.SearchRequest(nameinput, str(date1_input), str(date2_input))
    sas = service_searchapp.SearchAppService()
    tic = time.perf_counter()
    username_response = sas.search_users(sr)
    counter = 0
    for rr in username_response:
        st.write(f" {rr['user_screen_name']} >>> {rr['text']}")
        toc = time.perf_counter()
        st.write(f"**Time taken** {toc - tic:0.4f} seconds")
 
if(Page == 'Text'):
    st.write("""
    # Search Engine for Tweets

    This app finds Tweets and it's information based on type of search made by user.
    """)    
    textinput = st.text_input("Enter Text to be Searched")
    date1_input = st.date_input("Enter Start Date")
    date2_input = st.date_input("Enter End Date")
    sr = dm.SearchRequest(textinput, str(date1_input), str(date2_input))
    sas = service_searchapp.SearchAppService()
    tic = time.perf_counter()
    text_response = sas.search_tweet_texts(sr)
    counter = 0
    for rr in text_response:
        st.write(f" {rr['user_screen_name']} >>> {rr['text']}")
        toc = time.perf_counter()
        st.write(f"**Time taken** {toc - tic:0.4f} seconds")
        drill_down_button = st.button("More Tweets from : " + rr['user_screen_name'], key = counter)
        counter += 1
        if(drill_down_button):
            mdbs = service_mongo.MongoDBService()
            tweet_responses = mdbs.get_drill_down_tweets(rr['user_screen_name'])
            for rr in tweet_responses:
                st.write(rr)
                


if(Page == 'Hashtag'):
    st.write("""
    # Search Engine for Tweets

    This app finds Tweets and it's information based on type of search made by user.
    """)
    hashtaginput = st.text_input("Enter Hashtag to be Searched")
    date1_input = st.date_input("Enter Start Date")
    date2_input = st.date_input("Enter End Date")
    sr = dm.SearchRequest(hashtaginput, str(date1_input), str(date2_input))
    sas = service_searchapp.SearchAppService()
    tic = time.perf_counter()
    hashtag_response = sas.search_hashtags(sr)
    counter = 0
    for rr in hashtag_response:
        st.write(rr)
    toc = time.perf_counter()
    st.write(f"**Time taken** {toc - tic:0.4f} seconds")
        
if(Page == 'Metrics'):
    st.write("""
    # Search Engine for Tweets

    This app finds Tweets and it's information based on type of search made by user.
    """)

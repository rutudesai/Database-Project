from ast import Not
import data_model
import redis
import app_constants
import json
import re
from datetime import datetime

class RedisService:

    def __init__(self) -> None:
        self.reddb = redis.Redis(
        host='localhost',
        port=6379, 
        password='')
        resp = self.reddb.set("redis_test_123",1)
        print("Redis is ready..", resp)

    def getSearchData(self, search_request:data_model.SearchRequest):
        return self.reddb.get(search_request.get_cache_key())

    def getSearchDataByKey(self, cache_key):
        return self.reddb.get(cache_key)        

    def storeSearchData(self, search_request:data_model.SearchRequest, data):
        self.reddb.set(search_request.get_cache_key(), data)
        self.reddb.expire(search_request.get_cache_key(), app_constants.RC_EXPIRE_FIVE_MINUTES)

    def storeSearchDataByKey(self, cache_key, data):
        self.reddb.set(cache_key, data)
        self.reddb.expire(cache_key, app_constants.RC_EXPIRE_FIVE_MINUTES)

    def cahce_latest_tweet(self, tweet_data):
        try: 
            match_pattern = re.findall(app_constants.RETWEET_REGEX_PATTERN, tweet_data["text"])
            tweet = data_model.CachedTweet(tweet_data["created_at"],
                                tweet_data["timestamp_ms"],
                                tweet_data["user"]["screen_name"],
                                tweet_data["text"], 
                                tweet_data["id"],  
                                tweet_data["user"]["id"])

            if len(match_pattern) > 0:
                self.cahce_latest_user(tweet_data, True)
            else:
                self.cahce_latest_user(tweet_data, False)
            
            if (tweet_data["entities"] is not None 
                    and tweet_data["entities"]["hashtags"] is not None
                    and len(tweet_data["entities"]["hashtags"])> 0):
                        self.cache_latest_hashtags(tweet_data["entities"]["hashtags"])
            
            self.store_tweet(tweet)
        except Exception as e:            
            print("Oops! cahce_latest_tweet error occured.", e)
            print(tweet_data)
            return 

    def cahce_latest_user(self, tweet_data, is_retweet):
        try:             
            user = data_model.CachedUser(tweet_data["user"]["id"],
                                tweet_data["user"]["screen_name"],
                                int(tweet_data["timestamp_ms"]),
                                tweet_data["user"]["followers_count"], 
                                tweet_data["user"]["friends_count"],   
                                tweet_data["user"]["favourites_count"], 
                                tweet_data["user"]["statuses_count"],
                                is_retweet)
            # tweet_json = json.loads(json.dumps(tweet, default=vars))
            # print(f"Tweet Id: {tweet.tweet_id}, User: {tweet.user_screen_name}")
            self.store_user_metrics(user, is_retweet)
        except Exception as e:            
            print("Oops! cahce_latest_user error occured.", e)
            print(tweet_data)
            return  

    def cache_latest_hashtags(self, hashtags):
        for htag in hashtags:
            htag_text = htag["text"]
            self.reddb.zincrby(app_constants.RC_HASHTAG_TWEET, 1, htag_text)
        self.set_key_expiry(app_constants.RC_HASHTAG_TWEET)
        self.purge_hashtag_metrics()


    def store_tweet(self, tweet):
        try:
            tweet_json =  json.dumps(tweet, default=vars) #json.loads(json.dumps(tweet, default=vars))
            dic = {str(tweet_json): int(tweet.timestamp_ms)}
            self.reddb.zadd(name = app_constants.RC_TWEET_LATEST, mapping = dic)
            self.purge_tweet()
            self.set_key_expiry(app_constants.RC_TWEET_LATEST)
        except Exception as e:            
            print("Oops! store_tweet error occured.", e)
            print(tweet)
            return     

    def purge_tweet(self):
        try:
            tweet_count = self.reddb.zcount(app_constants.RC_TWEET_LATEST, "-inf", "+inf") 
            if tweet_count > app_constants.RC_MAX_TWEET_COUNT:
                self.reddb.zpopmin(app_constants.RC_TWEET_LATEST, tweet_count - app_constants.RC_MAX_TWEET_COUNT)
        except Exception as e:            
            print("Oops! purge_tweet error occured.", e)
            return   

    def get_tweets(self):
        try:
            data = []
            result = self.reddb.zrange(app_constants.RC_TWEET_LATEST, 0, app_constants.RC_MAX_TWEET_COUNT, desc=True)
            for res in  result:
                temp = json.loads(res)
                
                temp["created_at"] = datetime.utcfromtimestamp(int(temp["timestamp_ms"])/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                temp.pop('timestamp_ms')
                temp.pop('tweet_id')
                temp.pop('user_id')
                data.append(temp) #json.loads(res)
            return data
        except Exception as e:            
            print("Oops! get_tweets error occured.", e)
            return  

    def get_user_metrics(self, cache_key):
        try:
            data = []
            result = self.reddb.zrange(cache_key, 0, app_constants.RC_MAX_USER_UI_COUNT, desc=True, withscores=True)
            for res in  result:                
                data.append({'user_screen_name': str(res[0].decode('utf-8')), 'count': int(res[1])}) #json.loads(res)
            return data
        except Exception as e:            
            print("Oops! get_user_metrics error occured.", e)
            return

    def get_hashtags(self):
        try:
            data = []
            result = self.reddb.zrange(app_constants.RC_HASHTAG_TWEET, 0, app_constants.RC_MAX_HASHTAG_UI_COUNT, desc=True, withscores=True)
            for res in  result:                
                data.append({'hashtag': str(res[0].decode('utf-8')), 'count': int(res[1])}) #json.loads(res)
            return data
        except Exception as e:            
            print("Oops! get_hashtags error occured.", e)
            return

    def store_user_metrics(self, user, is_retweet):

        existing_user_lastupdated_ts = self.reddb.get(user.user_id)

        if existing_user_lastupdated_ts is None or int(existing_user_lastupdated_ts) < user.timestamp_ms:
            self.reddb.set(user.user_id, int(user.timestamp_ms))
            self.set_key_expiry(user.user_id)

            self.reddb.zadd(name = app_constants.RC_USER_FOLLOWER_COUNT, mapping={str(user.user_screen_name): int(user.follow_count)})
            self.purge_user_metrics(app_constants.RC_USER_FOLLOWER_COUNT)
            self.set_key_expiry(app_constants.RC_USER_FOLLOWER_COUNT)

            self.reddb.zadd(name = app_constants.RC_USER_FRIENDS_COUNT, mapping={str(user.user_screen_name): int(user.friends_count)})
            self.purge_user_metrics(app_constants.RC_USER_FRIENDS_COUNT)
            self.set_key_expiry(app_constants.RC_USER_FRIENDS_COUNT)

            self.reddb.zadd(name = app_constants.RC_USER_FAV_COUNT, mapping={str(user.user_screen_name): int(user.fav_count)})
            self.purge_user_metrics(app_constants.RC_USER_FAV_COUNT)
            self.set_key_expiry(app_constants.RC_USER_FAV_COUNT)

            self.reddb.zadd(name = app_constants.RC_USER_STATUS_COUNT, mapping={str(user.user_screen_name): int(user.status_count)})
            self.purge_user_metrics(app_constants.RC_USER_STATUS_COUNT)
            self.set_key_expiry(app_constants.RC_USER_STATUS_COUNT)

            if is_retweet is False:
                self.reddb.zadd(name = app_constants.RC_USER_TWEET_COUNT, mapping={str(user.user_screen_name): 1}, incr=True)
                self.purge_user_metrics(app_constants.RC_USER_TWEET_COUNT)
                self.set_key_expiry(app_constants.RC_USER_TWEET_COUNT)
            else:
                self.reddb.zadd(name = app_constants.RC_USER_RETWEET_COUNT, mapping={str(user.user_screen_name): 1}, incr=True)
                self.purge_user_metrics(app_constants.RC_USER_RETWEET_COUNT)
                self.set_key_expiry(app_constants.RC_USER_RETWEET_COUNT)

    def purge_user_metrics(self, cache_key):
        try:
            max_count = self.reddb.zcount(cache_key, "-inf", "+inf") 
            if max_count > app_constants.RC_MAX_USER_COUNT:
                self.reddb.zpopmin(cache_key, max_count - app_constants.RC_MAX_USER_COUNT)
        except Exception as e:            
            print("Oops! purge_user_metrics error occured.", e)
            return 


    def purge_hashtag_metrics(self):
        try:
            max_count = self.reddb.zcount(app_constants.RC_HASHTAG_TWEET, "-inf", "+inf") 
            if max_count > app_constants.RC_MAX_HASHTAG_COUNT:
                self.reddb.zpopmin(app_constants.RC_HASHTAG_TWEET, max_count - app_constants.RC_MAX_HASHTAG_COUNT)
        except Exception as e:            
            print("Oops! purge_hashtag_metrics error occured.", e)
            return  
    
    def set_key_expiry(self, cache_key):
        self.reddb.expire(cache_key, app_constants.RC_EXPIRE_SIXTY_MINUTES)
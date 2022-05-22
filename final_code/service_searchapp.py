import utilities as utility
import service_redis as redisService;
import service_mongo as mongoService;
import service_pgsql as pgsqlService;
import data_model as dm
import json
import app_constants as apc

class SearchAppService:

    def __init__(self):
        self.redisService = redisService.RedisService()
        self.pgsqlService = pgsqlService.PGSQLService()
        self.mongoService = mongoService.MongoDBService()

    def search_tweet_texts(self, search_query: dm.SearchRequest):
        return self.perform_search(search_query)

    def search_users(self, search_query: dm.SearchRequest):
        return self.perform_user_search(search_query)

    def search_hashtags(self, search_query: dm.SearchRequest):
        return self.perform_hashtag_search(search_query)

    def get_drill_down_details(self, user_screen_name):
        return self.perform_user_drill_down_search(user_screen_name)

    def search_redis_cache(self, search_request: dm.SearchRequest):
        return self.redisService.getSearchData(search_request)  

    def store_redis_cache(self, search_request: dm.SearchRequest, data):
        return self.redisService.storeSearchData(search_request, data)

    def search_redis_cache_key(self, cache_key):
        return self.redisService.getSearchDataByKey(cache_key) 

    def store_redis_cache_key(self, cache_key, data):
        return self.redisService.storeSearchDataByKey(cache_key, data)

    def search_mongo_db(self, search_request: dm.SearchRequest):
        return self.mongoService.search_text(search_request)

    def get_relevance_pgsql_db(self, tweetids):
        return self.pgsqlService.sort_by_relavance(tweetids) 

    def perform_user_drill_down_search(self, user_screen_name):
        cache_key = ''.join([apc.RC_USER_DD,'|',user_screen_name])
        response = []
        # check if data exists in cache
        cache_response = self.search_redis_cache_key(cache_key)
        
        if cache_response is None:
            # if not in cache get from mongo
            mongo_response = self.mongoService.get_drill_down_tweets(user_screen_name)
            # sort for relevance from pgsql

            for tweet in mongo_response:
                response.append(tweet)
        
            # store response in cache
            response = self.format_to_cache(response)
            self.store_redis_cache_key(cache_key, response)
            return self.format_from_cache(response)
        else:
            # return result back
            return self.format_from_cache(cache_response)     
       
    def perform_hashtag_search(self, search_query):
        response = []
        # check if data exists in cache
        cache_response = self.search_redis_cache(search_query)
        
        if cache_response is None:
            # if not in cache get from mongo
            mongo_response = self.mongoService.search_hashtags(search_query)
            # sort for relevance from pgsql
            tweetids = []
            tweets = []
            for tweet in mongo_response:
                tweetids.append(tweet["_id"])
                tweets.append(tweet)
            if len(tweetids) > 0:
                tweetids = self.get_relevance_pgsql_db(tweetids)
            # sort mongo result based on relavance
            for t in tweetids:
                found = False
                for mt in tweets:
                    if mt["_id"] == t:
                        response.append(mt)
                        found = True
                        break
                if found == False:
                    response.append(t)

            if len(response) == 0:
                response = tweets # incase pgsql data is not available
        
            # store response in cache
            response = self.format_to_cache(response)
            self.store_redis_cache(search_query, response)
            return self.format_from_cache(response)
        else:
            # return result back
            return self.format_from_cache(cache_response)

    def perform_user_search(self, search_query):
        response = []
        # check if data exists in cache
        cache_response = self.search_redis_cache(search_query)
        
        if cache_response is None:
            # if not in cache get from mongo
            mongo_response = self.mongoService.search_user(search_query)
            # sort for relevance from pgsql
            tweetids = []
            tweets = []
            for tweet in mongo_response:
                tweetids.append(tweet["_id"])
                tweets.append(tweet)
            if len(tweetids) > 0:
                tweetids = self.get_relevance_pgsql_db(tweetids)
            # sort mongo result based on relavance
            for t in tweetids:
                found = False
                for mt in tweets:
                    if mt["_id"] == t:
                        response.append(mt)
                        found = True
                        break
                if found == False:
                    response.append(t)

            if len(response) == 0:
                response = tweets # incase pgsql data is not available
        
            # store response in cache
            response = self.format_to_cache(response)
            self.store_redis_cache(search_query, response)
            return self.format_from_cache(response)
        else:
            # return result back
            return self.format_from_cache(cache_response)


    def perform_search(self, search_query):
        response = []
        # check if data exists in cache
        cache_response = self.search_redis_cache(search_query)
        
        if cache_response is None:
            # if not in cache get from mongo
            mongo_response = self.search_mongo_db(search_query)
            # sort for relevance from pgsql
            tweetids = []
            tweets = []
            for tweet in mongo_response:
                tweetids.append(tweet["_id"])
                tweets.append(tweet)
            if len(tweetids) > 0:
                tweetids = self.get_relevance_pgsql_db(tweetids)
            # sort mongo result based on relavance
            for t in tweetids:
                found = False
                for mt in tweets:
                    if mt["_id"] == t:
                        response.append(mt)
                        found = True
                        break
                if found == False:
                    response.append(t)

            if len(response) == 0:
                response = tweets # incase pgsql data is not available
        
            # store response in cache
            response = self.format_to_cache(response)
            self.store_redis_cache(search_query, response)
            return self.format_from_cache(response)
        else:
            # return result back
            return self.format_from_cache(cache_response)

    def format_from_cache(self, data_json):
        return json.loads(data_json)
    
    def format_to_cache(self, data):
        return json.dumps(data)

    def filter_remove_indexes(self, results):
        response = []
        for result in results:
            result.pop("idx_user_screen_name")
            result.pop("hashtags")
            response.append(result)
        return response        

# import time
# tic = time.perf_counter()
# sr =  dm.SearchRequest("'#Bo'","4/12/2020","4/25/2020")
# # # print(sr.get_cache_key())
# sas = SearchAppService()
# response = sas.search_tweet_texts(sr)
# for rr in response:
#     print(f" {rr['user_screen_name']} >>> {rr['text'][0:1000]}")
# toc = time.perf_counter()
# print(f"Time taken {toc - tic:0.4f} seconds")

# tic = time.perf_counter()
# response = sas.get_drill_down_details("IamRaavin")
# for rr in response:
#     print(f" {rr['user_screen_name']} >>> {rr['text'][0:10]}")
# toc = time.perf_counter()
# print(f"Time taken {toc - tic:0.4f} seconds")

import time
tic = time.perf_counter()
sr =  dm.SearchRequest("#Bo #pazar","4/12/2020","4/25/2020")
# # print(sr.get_cache_key())
sas = SearchAppService()
response = sas.search_hashtags(sr)
for rr in response:
    print(f" {rr['user_screen_name']} >>> {rr['text'][0:10]}")
toc = time.perf_counter()
print(f"Time taken {toc - tic:0.4f} seconds")
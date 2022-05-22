from pymongo import errors
from pymongo import MongoClient
from pprint import pprint
import app_constants as apc
import data_model as dataModel
import utilities as utility
import json

class MongoDBService:

    def __init__(self):
        self.client = MongoClient(apc.MONGO_CON_URL)
        db=self.client.admin
        # Issue the serverStatus command and print the results
        serverStatusResult=db.command("serverStatus")
        # pprint(serverStatusResult)
        self.twitterDB = self.client[apc.MONGO_DATABASE]
        self.tweets_collection = self.twitterDB[apc.MONGO_COLLECTION]

    def __enter__(self):
        return self                    

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client is not None:
            self.client.close()
            print('Mongo Database connection closed.')    
    
    def process_tweet(self, tweet_json):
        try:
            isReTweet = utility.isReTweet(tweet_json["text"])
            parent_tweet_id = None
            if isReTweet:
                if "retweeted_status" in tweet_json and tweet_json["retweeted_status"]["id"] is not None:
                    parent_tweet_id = int(tweet_json["retweeted_status"]["id"])
            tweet_id = tweet_json["id"]
            htags = []
            if (tweet_json["entities"] is not None 
                and tweet_json["entities"]["hashtags"] is not None
                and len(tweet_json["entities"]["hashtags"])> 0):
                    for htag in tweet_json["entities"]["hashtags"]:
                        # htags.append(''.join(['#', htag["text"]]))
                        htags.append(''.join(['000000', htag["text"]]))

            htags_json = ' '.join(htags)
            idx_user_screen_name = ''.join(['111111', tweet_json["user"]["screen_name"]])
            tweet = dataModel.MongoTweet(tweet_json["created_at"], int(tweet_json["timestamp_ms"]), 
                                            tweet_json["user"]["screen_name"], tweet_json["text"],
                                            int(tweet_json["user"]["id"]), isReTweet, 
                                            parent_tweet_id, htags_json, idx_user_screen_name)
            self.store_tweet(tweet, tweet_id)

            return True
        except (Exception) as error:
            print("Oops! MongoDBService process_tweet error occured.", error)
            print("tweet id", tweet_json["id"])
        
        return False

    def store_tweet(self, tweet, tweet_id):
        try:
            tweet_doc = json.loads(json.dumps(tweet, default=vars))

            doc_id_filter = "{\"_id\": " + str(tweet_id) + "}"
            
            self.tweets_collection.replace_one(json.loads(doc_id_filter), tweet_doc, True)
        
        except (Exception, errors.PyMongoError) as error:
            print("Oops! MongoDBService store_tweet error occured.", error)
            print("tweet id", str(tweet_id))

    def unique_hashtag(self):
        unique_hash = set()
        result = self.tweets_collection.distinct("hashtags")
        for rr in result:
            for ht in str(rr).split():
                unique_hash.add(ht)
        print(len(unique_hash))

    def search_text(self, search_request: dataModel.SearchRequest):
        sd = utility.convertDateTimeToUnixTs(search_request.start_date)
        ed = utility.convertDateTimeToUnixTs(search_request.end_date)
        find_filter = apc.MONGO_TWEET_SEARCH.replace("<<<search>>>", search_request.search_query)
        find_filter = find_filter.replace("<<<gte>>>", str(sd)).replace("<<<lte>>>", str(ed))
        # self.tweets_collection
        filter_json = json.loads(find_filter)
        result = self.tweets_collection.find(filter_json).limit(apc.MONGO_MAX_RESULTS)
        # print(find_filter)
        # for r in result:
        #     print(r)
        return self.filter_remove_indexes(result)

    def search_hashtags(self, search_request: dataModel.SearchRequest):
        sd = utility.convertDateTimeToUnixTs(search_request.start_date)
        ed = utility.convertDateTimeToUnixTs(search_request.end_date)
        hashtags = []
        for flt in search_request.search_query.split():
            hashtags.append(''.join(['000000', flt.replace('#','')]))
        hashtags = ' '.join(hashtags)
        find_filter = apc.MONGO_TWEET_SEARCH.replace("<<<search>>>", hashtags)
        find_filter = find_filter.replace("<<<gte>>>", str(sd)).replace("<<<lte>>>", str(ed))
        filter_json = json.loads(find_filter)
        result = self.tweets_collection.find(filter_json).limit(apc.MONGO_MAX_RESULTS)
        return self.filter_remove_indexes(result)

    def search_user(self, search_request: dataModel.SearchRequest):
        sd = utility.convertDateTimeToUnixTs(search_request.start_date)
        ed = utility.convertDateTimeToUnixTs(search_request.end_date)
        usernames = []
        for flt in search_request.search_query.split():
            usernames.append(''.join(['111111', flt.replace('#','')]))
        usernames = ' '.join(usernames)
        find_filter = apc.MONGO_TWEET_SEARCH.replace("<<<search>>>", usernames)
        find_filter = find_filter.replace("<<<gte>>>", str(sd)).replace("<<<lte>>>", str(ed))
        filter_json = json.loads(find_filter)
        result = self.tweets_collection.find(filter_json).limit(apc.MONGO_MAX_RESULTS)
        return self.filter_remove_indexes(result)        

    def get_drill_down_tweets(self, user_screen_name):
        find_filter = apc.MONGO_USER_TWEET_SEARCH.replace("<<<usn>>>", user_screen_name)        
        filter_json = json.loads(find_filter)
        result = self.tweets_collection.find(filter_json).sort(apc.MONGO_SORT_TS, -1)
        # print(find_filter)
        # for r in result:
        #     print(r)
        return self.filter_remove_indexes(result)

    def filter_remove_indexes(self, results):
        response = []
        for result in results:
            result.pop("idx_user_screen_name")
            result.pop("hashtags")
            response.append(result)
        return response

    # def hashtag_text(self, search_request: dataModel.SearchRequest):

    #     sd = utility.convertDateTimeToUnixTs(search_request.start_date)
    #     ed = utility.convertDateTimeToUnixTs(search_request.end_date)
    #     find_filter = apc.MONGO_TWEET_SEARCH.replace("<<<search>>>", search_request.search_query)
    #     find_filter = find_filter.replace("<<<gte>>>", str(sd)).replace("<<<lte>>>", str(ed))
    #     # self.tweets_collection
    #     filter_json = json.loads(find_filter)
    #     result = self.tweets_collection.find(filter_json).limit(apc.MONGO_MAX_RESULTS)
    #     # print(find_filter)
    #     # for r in result:
    #     #     print(r)
    #     return result
    

# ss = dataModel.SearchRequest("000000pazar 000000Bo", "04/12/2020", "04/25/2020")
# mdbs = MongoDBService()
# result = mdbs.search_text(ss)
# for r in result:
#     print(r['timestamp_ms'], r['text'])
#1249405611822981122 DiyoruzCom


# mdbs = MongoDBService()
# result = mdbs.get_drill_down_tweets("IamRaavin")

# for r in result:
#     print(r['timestamp_ms'], r['text'])


# ss = dataModel.SearchRequest("charlesmire", "04/12/2020", "04/25/2020")
# mdbs = MongoDBService()
# result = mdbs.search_user(ss)
# # result = mdbs.filter_remove_indexes(result)
# for r in result:
#     print(r)

# mdbs = MongoDBService()
# mdbs.unique_hashtag()
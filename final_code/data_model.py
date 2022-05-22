import utilities as utility

class PGSqlTweet:
    def encode(self):
        return self.__dict__
    def __init__(self, tweet_id, tweet_text, tweet_quote_count, tweet_reply_count, tweet_retweet_count, tweet_fav_count, tweet_is_retweet, tweet_timestamp_ms, user_id, user_name, user_screen_name, user_location, user_follow_count, user_friends_count, user_listed_count, user_fav_count, user_statuses_count, user_created_at, parent_tweet_id):
        self.tweet_id = tweet_id
        self.tweet_text = tweet_text
        self.tweet_quote_count = tweet_quote_count
        self.tweet_reply_count = tweet_reply_count
        self.tweet_retweet_count = tweet_retweet_count
        self.tweet_fav_count = tweet_fav_count
        self.tweet_is_retweet = tweet_is_retweet
        self.user_id = user_id
        self.tweet_created_at = tweet_timestamp_ms

        self.tweet_user_id = user_id
        self.user_name = user_name
        self.user_screen_name = user_screen_name
        self.user_location = user_location
        self.user_follow_count = user_follow_count
        self.user_friends_count = user_friends_count
        self.user_listed_count = user_listed_count
        self.user_fav_count = user_fav_count
        self.user_statuses_count = user_statuses_count
        self.user_created_at = user_created_at

        self.parent_tweet_id = parent_tweet_id


class MongoTweet:
    def encode(self):
        return self.__dict__
    def __init__(self, created_at, timestamp_ms, user_screen_name, text, 
                user_id, isReTweet, original_tweet_id, hashtags, idx_user_screen_name):
        self.created_at = created_at
        self.timestamp_ms = timestamp_ms
        self.user_screen_name = user_screen_name
        self.text = text
        self.user_id = user_id
        self.isReTweet = isReTweet
        self.original_tweet_id = original_tweet_id
        self.hashtags = hashtags
        self.idx_user_screen_name = idx_user_screen_name

class CachedTweet:
    def encode(self):
        return self.__dict__
    def __init__(self, created_at, timestamp_ms, user_screen_name, text, tweet_id, user_id):
        self.created_at = created_at
        self.timestamp_ms = timestamp_ms
        self.user_screen_name = user_screen_name
        self.text = text
        self.tweet_id = tweet_id
        self.user_id = user_id

class CachedHashtag:        
    def encode(self):
        return self.__dict__
    def __init__(self, tag, timestamp_ms, count):
        self.tag = tag
        self.timestamp_ms = timestamp_ms
        self.count = count


class CachedUser:
    def encode(self):
        return self.__dict__
    def __init__(self, user_id, user_screen_name, timestamp_ms, follow_count, friends_count, fav_count, status_count, is_retweet):
        self.user_id = user_id
        self.user_screen_name = user_screen_name
        self.timestamp_ms = timestamp_ms
        self.follow_count = follow_count
        self.friends_count = friends_count
        self.fav_count = fav_count
        self.status_count = status_count
        self.is_retweet = is_retweet


class SearchRequest:
    def __init__(self, search_query, start_date, end_date):
        self.search_query = search_query
        self.start_date = start_date
        self.end_date = end_date
        self.cache_key = self.set_cache_key()

    def set_cache_key(self):
        query_key = utility.gen_hash_cache_key(self.search_query)
        start_date_ts = str(utility.convertDateTimeToUnixTs(self.start_date))
        end_date_ts = str(utility.convertDateTimeToUnixTs(self.end_date))
        return ''.join([query_key, '|', start_date_ts, '|', end_date_ts])
    
    def get_cache_key(self):
        return self.cache_key
import datetime
import re
import app_constants as appConstants
import hashlib
import calendar
from dateutil import parser
import time

def isReTweet(tweet_text):
    match_pattern = re.findall(appConstants.RETWEET_REGEX_PATTERN, tweet_text)
    if len(match_pattern) > 0:
        return True
    else:
        return False

def convertDateTimeToUnixTs(created_at):
    date = parser.parse(created_at)
    unix_timestamp = datetime.datetime.timestamp(date)*1000
    return int(unix_timestamp)

# convertDateTimeToUnixTs(None)

def gen_hash_cache_key(search_query):
    lwr_str = str.lower(search_query)
    tokenized_str = str.split(lwr_str,' ')
    non_hash_key = ''.join(sorted(tokenized_str))
    return hashlib.sha256(non_hash_key.encode('utf-8')).hexdigest()

def get_date_to_unix_ts(date_value):
    unix_timestamp = datetime.datetime.timestamp(date_value)*1000
    return int(unix_timestamp)

# print(get_date_to_unix_ts(datetime.datetime.now()))
# print(convertDateTimeToUnixTs(str("Sun Apr 12 18:34:49 +0000 2020")))
# 1586716489255

# datetime.strptime('Sun Apr 12 18:34:49 +0000 2020', '%b %d %Y %I:%M%p')
# d = datetime.datetime.strptime('Sun Apr 12 18:34:49 +0000 2020', '%a %b %d %H:%M:%S %z %Y')
# unix_timestamp = datetime.datetime.timestamp(d)*1000
# print(unix_timestamp)
# print(convertDateTimeToUnixTs(str("Sun Apr 12 18:34:49 +0000 2020")))
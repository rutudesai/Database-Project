import psycopg2
import data_model as dataModel
import app_constants as appConstants
import utilities as utility

class PGSQLService:

    def __init__(self):
        self.conn = psycopg2.connect(
                    host="localhost",
                    database="twitter",
                    user="postgres",
                    password="postgres")

    def __enter__(self):
        return self                    

    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn is not None:
            self.conn.close()
            print('Database connection closed.')    

    def test_connection(self):
        try:
            # create a cursor
            cur = self.conn.cursor()
            
            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)
            
            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Oops! PGSQLService test_connection error occured.", error)
        finally:
            if self.conn is not None:
                self.conn.close()
                print('PGSQL database connection closed.')     

    def process_tweet(self, tweet_json):
        try:
            isReTweet = utility.isReTweet(tweet_json["text"])
            parent_tweet_id = None
            if isReTweet:
                if "retweeted_status" in tweet_json and tweet_json["retweeted_status"]["id"] is not None:
                    parent_tweet_id = int(tweet_json["retweeted_status"]["id"])
            user_created_at = utility.convertDateTimeToUnixTs(tweet_json["user"]["created_at"])

            psqlTweet = dataModel.PGSqlTweet(int(tweet_json["id"]), tweet_json["text"], int(tweet_json["quote_count"]), int(tweet_json["reply_count"]),
                                            int(tweet_json["retweet_count"]), int(tweet_json["favorite_count"]), isReTweet, int(tweet_json["timestamp_ms"]),
                                            int(tweet_json["user"]["id"]), tweet_json["user"]["name"], tweet_json["user"]["screen_name"], tweet_json["user"]["location"], 
                                            int(tweet_json["user"]["followers_count"]), int(tweet_json["user"]["friends_count"]), int(tweet_json["user"]["listed_count"]), 
                                            int(tweet_json["user"]["favourites_count"]), int(tweet_json["user"]["statuses_count"]), 
                                            user_created_at, parent_tweet_id)
            self.store_tweet(psqlTweet)
            return True
        except (Exception) as error:
            print("Oops! PGSQLService process_tweet error occured.", error)
            print("tweet id", tweet_json["id"])
        
        return False


    def store_tweet(self, tweet):
        try:
            # create a cursor object for execution
            cur = self.conn.cursor()

            # call a stored procedure
            cur.execute('CALL add_tweet(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (tweet.tweet_id, tweet.tweet_text, tweet.tweet_quote_count,
                                        tweet.tweet_reply_count, tweet.tweet_retweet_count, tweet.tweet_fav_count,
                                        tweet.tweet_is_retweet, tweet.tweet_created_at,
                                        tweet.tweet_user_id, tweet.user_name, tweet.user_screen_name, tweet.user_location, 
                                        tweet.user_follow_count, tweet.user_friends_count, tweet.user_listed_count, 
                                        tweet.user_fav_count, tweet.user_statuses_count, tweet.user_created_at,
                                        tweet.parent_tweet_id))
            # commit the transaction
            self.conn.commit()

            # close the cursor
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Oops! PGSQLService store_tweet error occured.", error)
        finally:
            pass

    def sort_by_relavance(self, tweetids):
        response = []
        try:
            # create a cursor object for execution
            cur = self.conn.cursor()

            # call a stored procedure
            cur.execute('SELECT * FROM tweet_sort(%s)', (tweetids,))
            
            # process the result set
            row = cur.fetchone()
            while row is not None:
                response.append(row[0])
                # print(row)
                row = cur.fetchone()

            # commit the transaction
            self.conn.commit()

            # close the cursor
            cur.close()
            return response
        except (Exception, psycopg2.DatabaseError) as error:
            print("Oops! PGSQLService store_tweet error occured.", error)
        finally:
            return response      


# def test_PGSQLService():
#     pgsqlService = PGSQLService()
#     pgsqlService.test_connection()


# test_PGSQLService()


# def test_PGSQLService():
#     # tweetids = [1254022772877131777,1254022774521081856,1254024716899291137,1254024115859517441,1254034810231717889]
#     tweetids =[1249405001346187264, 1249406807652655108, 1249404441754820613, 1249406404013764608, 1249405224801808384, 1249405199153803265, 1249408719433031683, 1249405609159430145, 1249407693523161088, 1249408267891044355, 1249407808602173443, 1249407055921909761, 1249404952658788353, 1249403878304432128]
#     pgsqlService = PGSQLService()
#     resp = pgsqlService.sort_by_relavance(tweetids)
#     print(resp)


# test_PGSQLService()
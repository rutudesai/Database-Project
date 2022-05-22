import pymongo
from pymongo import MongoClient
from pprint import pprint
import app_constants as apc

class MongoDBSetup:

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

    def setup_collection(self, mongo_client):
        # MongoDB waits until you have inserted a document before it actually creates the collection.
        mydb = mongo_client["twitter"]
        mycol = mydb["tweets"]
        return mycol

    def setup_indexes(self, ):
        self.tweets_collection.create_index([("timestamp_ms",-1)])
        self.tweets_collection.create_index([("text", pymongo.TEXT),("idx_user_screen_name", pymongo.TEXT),("hashtags", pymongo.TEXT)])


with MongoDBSetup() as mongoDB:
    mongoDB.setup_indexes()


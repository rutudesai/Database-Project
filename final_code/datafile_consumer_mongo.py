import service_datafile as dfservice
import service_mongo as mongoservice

def begin_data_load():
    try:
        datafileservice = dfservice.DataFileService()
        with mongoservice.MongoDBService() as mongoDBservice:
            datafileservice.processTwitterDataFromFile(mongoDBservice.process_tweet)
    except (Exception) as error:
            print("Oops! Mongo begin_data_load error occured.", error)
    
    print("Data file load into mongo completed.")


begin_data_load()
import service_datafile as dfservice
import service_redis as redisService

def begin_data_load():
    try:
        datafileservice = dfservice.DataFileService()
        redisservice = redisService.RedisService()
        datafileservice.processTwitterDataFromFile(redisservice.cahce_latest_tweet)
    except (Exception) as error:
        print("Oops! Redis begin_data_load error occured.", error)

    print("Data file load into Redis completed.")

begin_data_load()
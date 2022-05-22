import service_datafile as dfservice
import service_kafka_producer as kafkaProducerService

def begin_data_load():
    try:
        datafileservice = dfservice.DataFileService()
        kafkaProducer = kafkaProducerService.KafkaProducerService() 
        datafileservice.processTwitterDataFromFile(kafkaProducer.produce_tweet)
    except (Exception) as error:
            print("Oops! Kafka producer begin_data_load error occured.", error)
    
    print("Data file load into kafka completed.")


begin_data_load()
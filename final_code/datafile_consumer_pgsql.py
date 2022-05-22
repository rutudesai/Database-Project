import service_datafile as dfservice
import service_pgsql as pgservice

def begin_data_load():
    try:
        datafileservice = dfservice.DataFileService()
        with pgservice.PGSQLService() as pgsqlservice:
            datafileservice.processTwitterDataFromFile(pgsqlservice.process_tweet)
    except (Exception) as error:
        print("Oops! PostgreSQL begin_data_load error occured.", error)

    print("Data file load into PostgreSQL completed.")

begin_data_load()
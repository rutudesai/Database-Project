import json
import app_constants as  apc

class DataFileService:

    def processTwitterDataFromFile(self, datarowmapper):
        count = 0
        input_file = apc.DATA_FILE_3
        with open(input_file, "r") as f1:
            for line in f1:
                try: 

                    if len(line) < 2: # skip row if its empty line.
                        continue
                    count = count + 1
                    data = json.loads(line)
                    if datarowmapper(data) == False: # call the data row mapper with the raw data                          
                        break
                except Exception as e:                    
                    print("Oops! DataFileService processTwitterDataFromFile error occured.", e)
                    print(line)
                    break

                
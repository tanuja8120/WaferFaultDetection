from logger import App_Logger
from Azure_methods import Azure_Functions
import pymongo
import json
from io import StringIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import pandas as pd



class MongodBOperation:



    def __init__(self):
        self.logger = App_Logger()
        self.connectionstrings = "DefaultEndpointsProtocol=https;AccountName=trainingbatchfiles;AccountKey=JPHQiUP+0kPN4UlfW+jXZm9EaPg0nsSUd9MZMLnhpjmJZnO7OXiemYqM+vosRjXA8MLOTqV2fsDEAmz6tIjGFw==;EndpointSuffix=core.windows.net"
        self.AzureFunc = Azure_Functions(self.connectionstrings)
        self.client = pymongo.MongoClient("mongodb+srv://demo:test@rahulcluster.96p5y.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        self.db = self.client.test



    def dataBaseConnection(self):

        try:

            self.client = pymongo.MongoClient("mongodb+srv://demo:test@rahulcluster.96p5y.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
            self.db = self.client.test
            self.logger.log("Prediction_Log", "DataBaseConnectionLog", "Opened %s database successfully" % DatabaseName)

        except Exception as e:
            self.logger.log("Prediction_Log","DataBaseConnectionLog", "Error while connecting to database: %s" %ConnectionError)


    def createcollectionDB(self,DatabaseName):



        try:

            db = self.client["DatabaseName"]
            self.collection = db["predgood_data"]
            self.logger.log("Prediction_Log", "DbcollectionCreateLog", "collection created successfully!!")


        except Exception as e:
            self.logger.log("Prediction_Log","DbcollectionCreateLog" ,"Error while creating collection: %s " % e)
            raise e


    def insertIntoTableGoodData(self):

        try:

            filename = self.AzureFunc.gettingcsvfile("predictiongoodraw")
            for file in filename:
                df = self.AzureFunc.readingcsvfile("predictiongoodraw", file)
                js = df.to_json()
                jsdata = json.loads(js)
                self.collection.insert_one(jsdata)
                self.logger.log("Prediction_Log", "DbInsertLog", " %s: File loaded successfully!!" % file)

        except Exception as e:
            self.logger.log("Prediction_Log","DbInsertLog", "Error while inserting data into collection: %s " % e)



        # for files in self.AzureFunc.gettingcsvfile("predictiongoodraw"):
        #     csv = self.AzureFunc.readingcsvfile("predictiongoodraw", files)
        #     js = csv.to_json()
        #     jsdata = json.loads(js)
        #     self.collection.insert_one(jsdata)


    def selectingDatafromtableintocsv(self):

        try:


            li = []
            for i in self.collection.find():
                di = dict(i)
                di.pop('_id')
                jsda = json.dumps(di)
                json_data = json.loads(jsda)
                jfdf = pd.DataFrame(json_data.values()).T
                jfdf.columns = json_data.keys()
                li.append(jfdf)

            frame = pd.concat(li, ignore_index=True)
            upload = frame.to_csv()
            self.AzureFunc.uploadBlob("predinputdata", "predinputdata.csv", upload)
            self.collection.drop()
            print(frame)
            self.logger.log("Prediction_Log", "ExportToCsv", "File exported successfully on Azure !!!")

        except Exception as e:
            self.logger.log("Prediction_Log","ExportToCsv", "File exporting failed. Error : %s" %e)


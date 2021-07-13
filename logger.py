import pymongo
from datetime import datetime


class App_Logger:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb+srv://demo:test@rahulcluster.96p5y.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        self.db = self.client.test
        pass




    def log(self,db_name,collect_name, log_message):
        db = self.client[db_name]
        self.collection = db[collect_name]
        now = datetime.now()
        date = now.date()
        current_time = now.strftime("%H:%M:%S")
        d = {str(date) + "/" + str(current_time): log_message}
        self.collection.insert_one(d)
        # print(str(date) + "/" + str(current_time) + "\t" + log_message)

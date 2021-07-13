import pandas as pd
from Azure_methods import Azure_Functions

class Data_Getter:



    def __init__(self, file_object, logger_object):
        self.connectionstrings = "DefaultEndpointsProtocol=https;AccountName=trainingbatchfiles;AccountKey=JPHQiUP+0kPN4UlfW+jXZm9EaPg0nsSUd9MZMLnhpjmJZnO7OXiemYqM+vosRjXA8MLOTqV2fsDEAmz6tIjGFw==;EndpointSuffix=core.windows.net"
        self.AzureFunc = Azure_Functions(self.connectionstrings)
        self.training_file = "inputdata.csv"
        self.file_object=file_object
        self.logger_object=logger_object


    def get_data(self):


        self.logger_object.log(self.file_object, "ModelTrainingLog",'Entered the get_data method of the Data_Getter class')
        try:

            self.data = self.AzureFunc.readingcsvfile("inputdata",self.training_file)
            self.logger_object.log(self.file_object,"ModelTrainingLog",'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,"ModelTrainingLog",'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            self.logger_object.log(self.file_object,"ModelTrainingLog",'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()
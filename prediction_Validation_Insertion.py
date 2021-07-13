from predictionDataValidation import Prediction_Data_validation
from DataTransformationPrediction import dataTransformPredict
import json
from DataTypeValidationPrediction import MongodBOperation
from logger import App_Logger
from mailing import mail


class pred_validation:


    def __init__(self,path):
        self.raw_data = Prediction_Data_validation(path)
        self.dataTransform = dataTransformPredict()
        self.dBOperation = MongodBOperation()
        self.file_object = "Prediction_Logs"
        self.mail = mail("kashyaprahul2893@gmail.com")
        self.log_writer = App_Logger()


    def prediction_validation(self):


        try:
            self.log_writer.log(self.file_object,"Prediction_Log",'Start of Validation on files for prediction!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesfromschema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files

            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile,self.raw_data)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object,"Prediction_Log", "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object,"Prediction_Log", ("Starting Data Transforamtion!!"))
            self.dataTransform.replaceMissingWithNull()
            self.log_writer.log(self.file_object,"Prediction_Log", "DataTransformation Completed!!!")

            self.log_writer.log(self.file_object,"Prediction_Log", "Creating Prediction_Database and collection on the basis of given schema!!!")
            self.dBOperation.createcollectionDB("predictionfiles")
            self.log_writer.log(self.file_object,"Prediction_Log", "collection creation Completed!!")
            self.log_writer.log(self.file_object,"Prediction_Log", "Insertion of Data into collection started!!!!")


            self.dBOperation.insertIntoTableGoodData()
            self.log_writer.log(self.file_object,"Prediction_Log", "Insertion in collection completed!!!")

            self.log_writer.log(self.file_object,"Prediction_Log", "Validation Operation completed!!")
            self.log_writer.log(self.file_object,"Prediction_Log" ,"Extracting csv file from collection")
            self.dBOperation.selectingDatafromtableintocsv()

        except Exception as e:
            raise e














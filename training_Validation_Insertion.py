from rawValidation import Raw_Data_validation
from DataTransformation import dataTransform
import json
from DataTypeValidationTraining import MongodBOperation
from logger import App_Logger


class train_validation:


    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)
        self.dataTransform = dataTransform()
        self.dBOperation = MongodBOperation()
        self.file_object = ("Training_Logs")
        self.log_writer = App_Logger()


    def train_validation(self):
        try:
            self.log_writer.log(self.file_object,"Training_Main_Log" ,'Start of Validation on files!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesfromschema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files

            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile,self.raw_data)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object, "Training_Main_Log","Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object,"Training_Main_Log", "Starting Data Transforamtion!!")
            self.dataTransform.replaceMissingWithNull()
            self.log_writer.log(self.file_object, "Training_Main_Log","DataTransformation Completed!!!")

            self.log_writer.log(self.file_object,"Training_Main_Log","Creating Training_Database and tables on the basis of given schema!!!")
            self.dBOperation.createcollectionDB("trainingfiles")
            self.log_writer.log(self.file_object,"Training_Main_Log", "Table creation Completed!!")

            self.dBOperation.insertIntoTableGoodData()
            self.log_writer.log(self.file_object,"Training_Main_Log" ,"Insertion in Table completed!!!")

            self.log_writer.log(self.file_object,"Training_Main_Log" ,"Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Training_Main_Log","Extracting csv file from table")
            self.dBOperation.selectingDatafromtableintocsv()


        except Exception as e:
            raise e
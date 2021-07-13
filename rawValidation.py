import pandas as pd
import json
import os
import shutil
import re
from Azure_methods import Azure_Functions
from logger import App_Logger
from os import listdir
from io import StringIO
import pymongo
from mailing import mail


class Raw_Data_validation:


    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'schema_training.json'
        self.connectionstrings = "DefaultEndpointsProtocol=https;AccountName=trainingbatchfiles;AccountKey=JPHQiUP+0kPN4UlfW+jXZm9EaPg0nsSUd9MZMLnhpjmJZnO7OXiemYqM+vosRjXA8MLOTqV2fsDEAmz6tIjGFw==;EndpointSuffix=core.windows.net"
        self.move_rawfile = Azure_Functions(self.connectionstrings)
        self.logger = App_Logger()
        self.mail = mail("kashyaprahul2893@gmail.com")

    def valuesfromschema(self):

        """
                        Method Name: valuesFromSchema
                        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                        Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                        On Failure: Raise ValueError,KeyError,Exception

                         Written By: Rahul Kashyap
                        Version: 1.0
                        Revisions: None

                                """

        try:

            client = pymongo.MongoClient("mongodb+srv://demo:test@rahulcluster.96p5y.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
            db = client.test
            db = client["Json_data"]
            if "schema_training" in db.list_collection_names():
                collection = db["schema_training"]
                for i in collection.find():
                    d = dict(i)
                    d.pop("_id")
                    js = json.dumps(d)
                    dic = json.loads(js)
                    pattern = dic['SampleFileName']
                    LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
                    LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
                    column_names = dic['ColName']
                    NumberofColumns = dic['NumberofColumns']
                    print("Successfully loaded data in local")
            elif "schema_training" not in db.list_collection_names():
                with open(self.schema_path, 'r') as f:
                    js = json.load(f)
                    f.close()
                    collection = db["schema_training"]
                    collection.insert_one(js)
                    for i in collection.find():
                        d = dict(i)
                        d.pop("_id")
                        js = json.dumps(d)
                        dic = json.loads(js)
                        pattern = dic['SampleFileName']
                        LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
                        LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
                        column_names = dic['ColName']
                        NumberofColumns = dic['NumberofColumns']
                        print("inerted data in mongo db and load in local system")

            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log("Training_Logs","valuesfromSchemaValidationLog",message)


        except ValueError:
            self.logger.log("Training_Logs","valuesfromSchemaValidationLog ","ValueError:Value not found inside schema_training.json")
            raise ValueError


        except KeyError:
            self.logger.log("Training_Logs","valuesfromSchemaValidationLog ", "KeyError:Key value error incorrect key passed")
            raise KeyError


        except Exception as e:
            self.logger.log("Training_Logs","valuesfromSchemaValidationLog ", str(e))
            raise e



        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):

        """
                                        Method Name: manualRegexCreation
                                        Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                                    This Regex is used to validate the filename of the training data.
                                        Output: Regex pattern
                                        On Failure: None

                                         Written By: iNeuron Intelligence
                                        Version: 1.0
                                        Revisions: None

                                                """

        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex





    def validationFileNameRaw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile,files):



        """
                            Method Name: validationFileNameRaw
                            Description: This function validates the name of the training csv files as per given name in the schema!
                                         Regex pattern is used to do the validation.If name format do not match the file is moved
                                         to Bad Raw Data folder else in Good raw data.
                            Output: None
                            On Failure: Exception

                             Written By: iNeuron Intelligence
                            Version: 1.0
                            Revisions: None

                        """


        # pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.

        # create new directories
        onlyfiles = [f for f in self.Batch_Directory]
        try:
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            self.move_rawfile.copytoanother(filename,"trainingfiles","goodraw")
                            self.logger.log("Training_Logs","nameValidationLog","Valid File name!! File moved to GoodRaw Folder :: %s" % filename)
                            #shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")

                        else:
                            self.move_rawfile.copytoanother(filename,"trainingfiles","badraw")
                            self.logger.log("Training_Logs","nameValidationLog","Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                            self.mail.Gmail("File is not Valid","Invalid File Name!! File moved to Bad Raw Folder",filename)
                            #shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    else:
                        self.move_rawfile.copytoanother(filename,"trainingfiles","badraw")
                        self.logger.log("Training_Logs", "nameValidationLog","Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                        self.mail.Gmail("File is not Valid", "Invalid File Name!! File moved to Bad Raw Folder",filename)
                        #shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                else:
                    self.move_rawfile.copytoanother(filename,"trainingfiles","badraw")
                    self.logger.log("Training_Logs", "nameValidationLog","Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    self.mail.Gmail("File is not Valid", "Invalid File Name!! File moved to Bad Raw Folder", filename)
                    #shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")

        except Exception as e:
            self.logger.log("Training_Logs", "nameValidationLog", "Error occured while validating FileName %s" % e)
            raise e


    def validateColumnLength(self, NumberofColumns):



        """
                                  Method Name: validateColumnLength
                                  Description: This function validates the number of columns in the csv files.
                                               It is should be same as given in the schema file.
                                               If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                               If the column number matches, file is kept in Good Raw Data for processing.
                                              The csv file is missing the first column name, this function changes the missing name to "Wafer".
                                  Output: None
                                  On Failure: Exception

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                              """
        try:
            self.logger.log("Training_Logs", "nameValidationLog", "Column Length Validation Started!!")
            for files in self.move_rawfile.gettingcsvfile("goodraw"):
                csv = self.move_rawfile.readingcsvfile("goodraw",files)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    self.move_rawfile.copyandDel(files,'goodraw','badraw')
                    #shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log("Training_Logs", "nameValidationLog", "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % files)
                    self.mail.Gmail("File is not Valid","Invalid Column Length for the file!! File moved to Bad Raw Folder",files)

            self.logger.log("Training_Logs", "nameValidationLog", "Column Length Validation Completed!!")
        except OSError:
            self.logger.log("Training_Logs", "nameValidationLog", "Error Occured while moving the file :: %s" % OSError)
            raise OSError
        except Exception as e:
            self.logger.log("Training_Logs", "nameValidationLog", "Error Occured:: %s" % e)
            raise e






    def validateMissingValuesInWholeColumn(self):




        """
                                          Method Name: validateMissingValuesInWholeColumn
                                          Description: This function validates if any column in the csv file has all values missing.
                                                       If all the values are missing, the file is not suitable for processing.
                                                       SUch files are moved to bad raw data.
                                          Output: None
                                          On Failure: Exception

                                           Written By: iNeuron Intelligence
                                          Version: 1.0
                                          Revisions: None

                                                      """
        try:

            self.logger.log("Training_Logs","missingValuesInColumn" ,"Missing Values Validation Started!!")

            for files in self.move_rawfile.gettingcsvfile("goodraw"):
                csv = self.move_rawfile.readingcsvfile("goodraw", files)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        self.move_rawfile.copyandDel(files,"goodraw","badraw")
                        # shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                        #             "Training_Raw_files_validated/Bad_Raw")
                        break
                if count == 0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    self.move_rawfile.saveDataFrameTocsv("goodraw", files, csv, index=None, header=True)


        except OSError:
            self.logger.log("Training_Logs","missingValuesInColumn" , "Error Occured while moving the file :: %s" % OSError)
            raise OSError
        except Exception as e:
            self.logger.log("Training_Logs","missingValuesInColumn" , "Error Occured:: %s" % e)
            raise e


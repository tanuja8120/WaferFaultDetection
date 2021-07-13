import pandas
from Azure_methods import Azure_Functions
from logger import App_Logger



class dataTransform:





    def __init__(self):
        self.connectionstrings = "DefaultEndpointsProtocol=https;AccountName=trainingbatchfiles;AccountKey=JPHQiUP+0kPN4UlfW+jXZm9EaPg0nsSUd9MZMLnhpjmJZnO7OXiemYqM+vosRjXA8MLOTqV2fsDEAmz6tIjGFw==;EndpointSuffix=core.windows.net"
        self.good_raw = Azure_Functions(self.connectionstrings)
        self.logger = App_Logger()

    def replaceMissingWithNull(self):

        try:
            onlyfiles = [f for f in self.good_raw.gettingcsvfile("goodraw")]
            for file in onlyfiles:
                csv = self.good_raw.readingcsvfile("goodraw",file)
                #csv = pandas.read_csv(self.goodDataPath + "/" + file)
                csv.fillna('NULL', inplace=True)
                # #csv.update("'"+ csv['Wafer'] +"'")
                # csv.update(csv['Wafer'].astype(str))
                csv['Wafer'] = csv['Wafer'].str[6:]
                self.good_raw.saveDataFrameTocsv("goodraw",file,csv,index=None,header=True)
                self.logger.log("Training_Logs","dataTransformLog" ," %s: File Transformed successfully!!" % file)
                #csv.to_csv(self.goodDataPath + "/" + file, index=None, header=True)

        except Exception as e:
            self.logger.log("Training_Logs","dataTransformLog", "Data Transformation failed because:: %s" % e)
            # log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")





from sklearn.model_selection import train_test_split
import data_loader
from preprocessing import Preprocessor
import clustering
import tuner
import pandas
import file_methods
import data_loader_prediction
#from data_preprocessing import clustering
#from best_model_finder import tuner
#from file_operations import file_methods
from logger import App_Logger
from predictionDataValidation import Prediction_Data_validation
from Azure_methods import Azure_Functions

#Creating the common Logging object


class prediction:

    def __init__(self,path):
        self.log_writer = App_Logger()
        self.file_object = "Prediction_Logs"
        self.connectionstrings = "DefaultEndpointsProtocol=https;AccountName=trainingbatchfiles;AccountKey=JPHQiUP+0kPN4UlfW+jXZm9EaPg0nsSUd9MZMLnhpjmJZnO7OXiemYqM+vosRjXA8MLOTqV2fsDEAmz6tIjGFw==;EndpointSuffix=core.windows.net"
        self.AzureFunc = Azure_Functions(self.connectionstrings)
        if path is not None:
            self.pred_data_val = Prediction_Data_validation(path)



    def predictionFromModel(self):

        try:


            self.pred_data_val.deletePredictionFile()
            self.log_writer.log(self.file_object,"Prediction_Log" ,'Start of Prediction')
            data_getter = data_loader_prediction.Data_Getter_Pred(self.file_object, self.log_writer)
            data = data_getter.get_data()



            """doing the data preprocessing"""

            preprocessor = Preprocessor(self.file_object, self.log_writer)
            data = preprocessor.remove_columns(data, ["Unnamed: 0"]) # remove the unnamed column as it doesn't contribute to prediction.


            is_null_present = preprocessor.is_null_present(data)

            # if missing values are there, replace them appropriately.
            if (is_null_present):data = preprocessor.impute_missing_values(data)  # missing value imputation

                # check further which columns do not contribute to predictions
                # if the standard deviation for a column is zero, it means that the column has constant values
                # and they are giving the same output both for good and bad sensors
                # prepare the list of such columns to drop
            cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(data)

                # drop the columns obtained above
            data = preprocessor.remove_columns(data, cols_to_drop)

            """ Applying the clustering approach"""

            file_loader = file_methods.File_Operation(self.file_object, self.log_writer)
            kmeans = file_loader.load_model('KMeans')

                ##Code changed
                # pred_data = data.drop(['Wafer'],axis=1)
            clusters = kmeans.predict(data.drop(['Wafer'], axis=1))  # drops the first column for cluster prediction
            data['clusters'] = clusters
            clusters = data['clusters'].unique()
            for i in clusters:
                cluster_data = data[data['clusters'] == i]
                wafer_names = list(cluster_data['Wafer'])
                cluster_data = data.drop(labels=['Wafer'], axis=1)
                cluster_data = cluster_data.drop(['clusters'], axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                result = list(model.predict(cluster_data))
                result = pandas.DataFrame(list(zip(wafer_names, result)), columns=['Wafer', 'Prediction'])
                #path = "Predictions.csv"

                pred_result = result.to_csv( header=True)
                self.AzureFunc.uploadBlob("predictionoutputfile","predictions.csv",pred_result)
                output = self.AzureFunc.readingcsvfile("predictionoutputfile", "predictions.csv")


                #result.to_csv("Predictions.csv", header=True,mode='a+')  # appends result to prediction file
            self.log_writer.log(self.file_object,"Prediction_Log" ,'End of Prediction')
        except Exception as ex:
            self.log_writer.log(self.file_object,"Prediction_Log", 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return output.head().to_json(orient="records")
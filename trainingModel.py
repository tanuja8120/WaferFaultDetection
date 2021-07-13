from sklearn.model_selection import train_test_split
import data_loader
from preprocessing import Preprocessor
import clustering
import tuner
import file_methods
#from data_preprocessing import clustering
#from best_model_finder import tuner
#from file_operations import file_methods
from logger import App_Logger

#Creating the common Logging object


class trainModel:


    try:


        def __init__(self):
            self.log_writer = App_Logger()
            self.file_object = "Training_Logs"



        def trainingModel(self):
            # Logging the start of Training
            self.log_writer.log(self.file_object,"ModelTrainingLog", 'Start of Training')



            # Getting the data from the source
            data_getter=data_loader.Data_Getter(self.file_object,self.log_writer)
            data=data_getter.get_data()



            """doing the data preprocessing"""

            preprocessor = Preprocessor(self.file_object, self.log_writer)
            data = preprocessor.remove_columns(data, ['Wafer',"Unnamed: 0"]) # remove the unnamed column as it doesn't contribute to prediction.

            # create separate features and labels
            X, Y = preprocessor.separate_label_feature(data, label_column_name='Good/Bad')

            is_null_present = preprocessor.is_null_present(X)

            # if missing values are there, replace them appropriately.
            if (is_null_present):X = preprocessor.impute_missing_values(X)  # missing value imputation

            # check further which columns do not contribute to predictions
            # if the standard deviation for a column is zero, it means that the column has constant values
            # and they are giving the same output both for good and bad sensors
            # prepare the list of such columns to drop
            cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(X)

            # drop the columns obtained above
            X = preprocessor.remove_columns(X, cols_to_drop)

            """ Applying the clustering approach"""

            kmeans = clustering.KMeansClustering(self.file_object, self.log_writer)  # object initialization.
            number_of_clusters = kmeans.elbow_plot(X)  # using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X = kmeans.create_clusters(X, number_of_clusters)

            # create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels'] = Y

            # getting the unique clusters from our dataset
            list_of_clusters = X['Cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data = X[X['Cluster'] == i]  # filter the data for one cluster

                # Prepare the feature and Label columns
                cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
                cluster_label = cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3,random_state=355)

                model_finder = tuner.Model_Finder(self.file_object, self.log_writer)  # object initialization

                # getting the best model for each of the clusters
                best_model_name, best_model = model_finder.get_best_model(x_train, y_train, x_test, y_test)

                # saving the best model to the directory.
                file_op = file_methods.File_Operation(self.file_object, self.log_writer)
                save_model = file_op.save_model(best_model, best_model_name + str(i))

            # logging the successful Training
            self.log_writer.log(self.file_object,"ModelTrainingLog" ,'Successful End of Training')
    except Exception:

        # logging the unsuccessful Training
        self.log_writer.log(self.file_object,"ModelTrainingLog", 'Unsuccessful End of Training')
        raise Exception



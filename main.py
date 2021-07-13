from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
from flask_cors import CORS, cross_origin
import flask_monitoringdashboard as dashboard
import json


import os, uuid
import dill
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import pandas as pd
from io import StringIO
import pickle
import jsonschema
from Azure_methods import Azure_Functions
from training_Validation_Insertion import train_validation
from trainingModel import trainModel
from prediction_Validation_Insertion import pred_validation
from predictFromModel import prediction








os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)



@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')







@app.route("/predict", methods=['GET'])
@cross_origin()
def predictRouteClient():

    a = Azure_Functions(connection_string="DefaultEndpointsProtocol=https;AccountName=trainingbatchfiles;AccountKey=JPHQiUP+0kPN4UlfW+jXZm9EaPg0nsSUd9MZMLnhpjmJZnO7OXiemYqM+vosRjXA8MLOTqV2fsDEAmz6tIjGFw==;EndpointSuffix=core.windows.net")
    b = a.gettingcsvfile("predictionbatchfiles")
    print(b)

    trainobj = pred_validation(b)
    trainobj.prediction_validation()

    pred = prediction(b)

    json_predictions = pred.predictionFromModel()
    return Response('and few of the predictions are '+str(json.loads(json_predictions) ))












@app.route("/train", methods=['GET'])
@cross_origin()
def trainRouteClient():

    try:

        a = Azure_Functions(connection_string="DefaultEndpointsProtocol=https;AccountName=trainingbatchfiles;AccountKey=JPHQiUP+0kPN4UlfW+jXZm9EaPg0nsSUd9MZMLnhpjmJZnO7OXiemYqM+vosRjXA8MLOTqV2fsDEAmz6tIjGFw==;EndpointSuffix=core.windows.net")
        b = a.gettingcsvfile("predictionbatchfiles")
        print(b)


        trainobj = train_validation(b)
        trainobj.train_validation()

        trainModelObj = trainModel()
        trainModelObj.trainingModel()


    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")

port = int(os.getenv("PORT",5000))
if __name__ == "__main__":
    host = '0.0.0.0'
    #port = 5000
    httpd = simple_server.make_server(host, port, app)
    # print("Serving on %s %d" % (host, port))
    httpd.serve_forever()



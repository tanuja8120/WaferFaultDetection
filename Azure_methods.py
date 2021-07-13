from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from io import StringIO
from logger import App_Logger


class Azure_Functions:
    connectionstrings = "DefaultEndpointsProtocol=https;AccountName=trainingbatchfiles;AccountKey=JPHQiUP+0kPN4UlfW+jXZm9EaPg0nsSUd9MZMLnhpjmJZnO7OXiemYqM+vosRjXA8MLOTqV2fsDEAmz6tIjGFw==;EndpointSuffix=core.windows.net"

    def __init__(self,connection_string):
        try:

            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            self.container_name = [i.name for i in self.blob_service_client.list_containers()]

        except Exception as e:
            raise Exception("Error while getting container name"+str(e))

    def gettingcsvfile(self,container_name):

        try:
            present_container = [i.name for i in self.blob_service_client.list_containers()]
            if container_name in present_container:
                self.client = self.blob_service_client.get_container_client(container_name)
                files = [files.name for files in self.client.list_blobs()]
                self.files = files
                return self.files

            else:

                self.blob_service_client.create_container(container_name)
                self.client = self.blob_service_client.get_container_client(container_name)
                files = [files.name for files in self.client.list_blobs()]
                self.files = files
                return self.files


        except Exception as e:
            raise Exception("Error while reading csv data from "+container_name+str(e))

    def copytoanother(self,files,container_name,dest_container_name):

        try:
            present_container = [i.name for i in self.blob_service_client.list_containers()]
            if dest_container_name in present_container:
                copy = self.blob_service_client.get_blob_client(container_name, files)
                url = copy.url
                dest_object = self.blob_service_client.get_blob_client(dest_container_name, files)
                dest_object.start_copy_from_url(url)

            else:
                self.blob_service_client.create_container(dest_container_name)
                copy = self.blob_service_client.get_blob_client(container_name, files)
                url = copy.url
                dest_object = self.blob_service_client.get_blob_client(dest_container_name, files)
                dest_object.start_copy_from_url(url)

        except Exception as e:
            raise Exception("getting Error while copying from "+container_name+ "to" +dest_container_name+ str(e))

    def create_container(self,container_name):
        self.createcontainer = self.blob_service_client.create_container(container_name)

    def fetch_goodRaw(self,container,blob):
        object_csv = [i for i in self.blob_service_client.get_blob_client(container,blob)]
        self.object_csv = object_csv
        return self.object_csv

    def readingcsvfile(self,container,files):
        Dframe = self.blob_service_client.get_blob_client(container, files)
        df = pd.read_csv(StringIO(Dframe.download_blob().readall().decode()))
        return df.copy()


    def copyandDel(self,files,container_name,dest_container_name):

        try:
            present_container = [i.name for i in self.blob_service_client.list_containers()]
            if dest_container_name in present_container:
                copy = self.blob_service_client.get_blob_client(container_name, files)
                url = copy.url
                dest_object = self.blob_service_client.get_blob_client(dest_container_name, files)
                dest_object.start_copy_from_url(url)
                copy.delete_blob()  

            else:

                self.blob_service_client.create_container(dest_container_name)
                copy = self.blob_service_client.get_blob_client(container_name, files)
                url = copy.url
                dest_object = self.blob_service_client.get_blob_client(dest_container_name,files)
                dest_object.start_copy_from_url(url)
                copy.delete_blob()
        except Exception as e:
            raise Exception("'Getting error while moving file from\t'"+container_name+ "'\tto\t'" +dest_container_name)

    def saveDataFrameTocsv(self, directory_name, file_name, data_frame,**kwargs):


        self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
        allowed_keys = ['index', 'header', 'mode']
        self.__dict__.update((k, v) for k, v in kwargs.items() if k in allowed_keys)

        directory_name = directory_name.lower()
        if file_name.split(".")[-1] != "csv":
            file_name = file_name + ".csv"
        if directory_name not in self.dir_list:
            self.createDirectory(directory_name)
        if file_name in self.gettingcsvfile(directory_name) and 'mode' in self.__dict__.keys():
            if self.mode == 'a+':
                df = self.readingcsvfile(directory_name=directory_name, file_name=file_name)
                data_frame = df.append(data_frame)
                if 'Unnamed: 0' in data_frame.columns:
                    data_frame = data_frame.drop(columns=['Unnamed: 0'], axis=1)

        if file_name in self.gettingcsvfile(directory_name):
            blob_client = self.blob_service_client.get_blob_client(container=directory_name, blob=file_name)
            blob_client.delete_blob()

        blob_client = self.blob_service_client.get_blob_client(container=directory_name, blob=file_name)
        if "index" in self.__dict__.keys() and "header" in self.__dict__.keys():
            output = data_frame.to_csv(encoding="utf-8", index=self.index, header=self.header)
        elif "header" in self.__dict__.keys() and 'mode' in self.__dict__.keys():
            output = data_frame.to_csv(encoding="utf-8", header=self.header)
        else:
            output = data_frame.to_csv(encoding="utf-8")
        blob_client.upload_blob(output)





    def createDirectory(self, directory_name, is_replace=False):

        self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]

        container_name = directory_name.lower()

        if container_name not in self.dir_list:
            self.blob_service_client.create_container(container_name)
        elif is_replace and container_name in self.dir_list:
            for file in self.gettingcsvfile(container_name):
                self.copyandDel(file, container_name, "recycle-bin")
        else:
            raise Exception(
                "Error occured in class: AzureBlobManagement method:createDirectory error: Directory alredy exists try to user is replace paremeter to true to delete and recreate directory")
        self.dir_list = [container_name.name for container_name in self.blob_service_client.list_containers()]



    def uploadBlob(self,container,blob,uploaderFile):

        present_container = [i.name for i in self.blob_service_client.list_containers()]
        if container in present_container:
            good_container = [i for i in self.gettingcsvfile(container)]
            if blob in good_container:
                pass
            else:
                bobclient = self.blob_service_client.get_blob_client(container, blob)
                bobclient.upload_blob(uploaderFile)
        else:
            self.blob_service_client.create_container(container)
            bobclient = self.blob_service_client.get_blob_client(container, blob)
            bobclient.upload_blob(uploaderFile)


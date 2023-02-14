#!/usr/bin/env python
# coding: utf-8

# In[17]:


import os
import json
from pathlib import Path
import pandas as pd
import mysql.connector
from mysql.connector import Error
from itertools import chain
import sqlalchemy


# In[2]:


class get_df:
    def update_dataset(self):
        dir_path = os.getcwd()
        file_folder = str(dir_path) + "\\pulse"
        if os.path.isdir(file_folder)==True:
            get_ipython().system('cd die_path')
            get_ipython().system('rmdir pulse /s /q')
            print("folder del")
        print("Downloading latest dataset")
        get_ipython().system('cd dir_path')
        get_ipython().system('git clone https://github.com/PhonePe/pulse')

    def get_all_files(self,folder):
        file_list = []
        if os.path.exists(folder):
            for root, dirs, files in os.walk(folder):
                for file in files:
                    file_list.append(os.path.join(root,file))
        return file_list

    def create_df(self,file_li,value):
        ldata = []
        col = []
        for i in file_li:

            #read the json file
            f = open(i)
            jdata = json.load(f)
            f.close()    

            #getting year and state from folder name
            state = Path(i).parts[-3]
            year = Path(i).parts[-2]
            file_name = Path(i).parts[-1]

            #Based on folder selecting data format to match json format
            if value == 0:
                for i in range(len(jdata["data"]["transactionData"])):
                    ldata.append([state, year, 
                                  jdata["data"]["from"], 
                                  jdata["data"]["to"], 
                                  jdata["data"]["transactionData"][i]["name"], 
                                  jdata["data"]["transactionData"][i]["paymentInstruments"][0]["type"], 
                                  jdata["data"]["transactionData"][i]["paymentInstruments"][0]["count"], 
                                  jdata["data"]["transactionData"][i]["paymentInstruments"][0]["amount"]
                                 ])
                col=['State','Year','From', 'To', 'Name', 'Type', 'Count', 'Amount']
            if value == 1:
                if jdata["data"]["usersByDevice"] != None:
                    for i in range(len(jdata["data"]["usersByDevice"])):
                        ldata.append([state, year, file_name,
                                          jdata["data"]["aggregated"]["registeredUsers"],
                                          jdata["data"]["aggregated"]["appOpens"],
                                          jdata["data"]["usersByDevice"][i]["brand"],
                                          jdata["data"]["usersByDevice"][i]["count"],
                                          jdata["data"]["usersByDevice"][i]["percentage"]
                                       ])
                col=['State','Year', 'Quater', 'Register Users', 'App opens', 'Brand', 'Count', 'Percentage']

            if value == 2:
                for i in range(len(jdata["data"]["hoverDataList"])):
                    ldata.append([
                                    state, year,
                                    jdata["data"]["hoverDataList"][i]["name"],
                                    jdata["data"]["hoverDataList"][i]["metric"][0]["type"],
                                    jdata["data"]["hoverDataList"][i]["metric"][0]["count"],
                                    jdata["data"]["hoverDataList"][i]["metric"][0]["amount"]
                                  ])
                col=['State','Year', 'Name', 'Type', 'Count', 'Amount']

            if value == 3:
                for i in jdata["data"]["hoverData"]:
                    ldata.append([
                                    state, year,
                                    jdata["data"]["hoverData"][i]["registeredUsers"],
                                    jdata["data"]["hoverData"][i]["appOpens"]
                                   ])
                col=['State','Year', 'Registered Users', 'App Opens']

            if value == 4:
                for i in jdata["data"]:
                    if jdata["data"][i] != None:
                        for j in range(len(jdata["data"][i])):
                            ldata.append([
                                            state,year,
                                            jdata["data"][i][j]["entityName"],
                                            jdata["data"][i][j]["metric"]["type"],
                                            jdata["data"][i][j]["metric"]["count"],
                                            jdata["data"][i][j]["metric"]["amount"]
                                           ])
                col=['State','Year', 'Entity Name', 'Type', 'Count', 'Amount']

            if value == 5:
                for i in jdata["data"]:
                    if jdata["data"][i] !=None:
                        for j in jdata["data"][i]:
                            ldata.append([        
                                        state,year,
                                        j["name"],
                                        j["registeredUsers"]
                                        ])
                col=['State','Year', 'Name', 'Register User']

        #Creating a dataframe and returning it to be stored in a list
        df = pd.DataFrame(ldata, columns=col) 
        return df

    def get_df_li(self):
        dir_path = os.getcwd()
        file_folder = str(dir_path) + "\\Pulse\\data"
        f1 = ["aggregated", "map", "top"]
        f2 = ["transaction", "user"]
        df_file_list = []
        for i in f1:
            for j in f2:
                temp_path = f"{file_folder}\{i}\{j}"
                df_file_list.append(self.get_all_files(temp_path))
        df_li = []
        for i in range(len(df_file_list)):
            df_li.append(self.create_df(df_file_list[i],i))

        return df_li


# In[3]:


class sql_fun():
    
    def __init__(self, host_name, user_name, user_password):
        self.host_name = host_name
        self.user_name = user_name
        self.user_password = user_password
        
    def create_server_connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(
            host = self.host_name,
            user = self.user_name,
            password = self.user_password)
            print("MySql connection Successful")
        except Error as err:
            print(f"Error : '{err}'")
        return connection

    def create_database(self,connection, query):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            print("Database created successfully")
        except Error as err:
            print(f"Error : '{err}'")

    def create_db_connection(self, db_name):
        connection = None
        try:
            connection = mysql.connector.connect(
            host = self.host_name,
            user = self.user_name,
            password = self.user_password,
            database = db_name)
            print("phonepe_db connectionn sucessful")
        except Error as err:
            print(f"Error : '{err}'")
        return connection

    def excute_query(self, connection, query):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
            print("Query was successful")
        except Error as err:
            print(f"Error : '{err}'")

    def read_query(self, connection, query):
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as err:
            print(f"Error : '{err}'")


# In[4]:


def check_conn(host_name, user_name, pw, db):
    
        #Establish server connection
        conn = sql_fun(host_name, user_name , pw)
        connection = conn.create_server_connection()

        #To check if database exsist
        query = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db}'"
        temp = read_query(connection, query)
        if len(temp)==0:
            create_db_query = "Create database phonepe_db"
            create_database(connection, create_db_query)

        #To check if table exsist
        table_list = ["agg_tran", "agg_user", "map_tran", "map_user", "top_tran", "top_user"]
        for i in table_list:
            query = f"SELECT * FROM information_schema.tables WHERE table_schema = '{db}' AND table_name = '{i}'"
            temp = read_query(connection, query)
            if len(temp)==0:
                x = "USE phonepe_db;"
                read_query(connection, x)
                if i == "agg_tran":
                    create_table_query = f"CREATE TABLE {i} (sr varchar(255), state varchar(255),year varchar(255) ,from_time varchar(255) ,to_time varchar(255),name varchar(255),type varchar(255), count varchar(255),amount varchar(255));"  
                if i == "agg_user":
                    create_table_query = f"CREATE TABLE {i} (state varchar(255),year varchar(255) ,quater varchar(255) ,reg_user varchar(255),app_opens varchar(255),brand varchar(255),count varchar(255), percentage varchar(255));"
                if i == "map_tran":
                    create_table_query = f"CREATE TABLE {i} (state varchar(255),year varchar(255) ,name varchar(255) ,type varchar(255),count varchar(255),amount varchar(255));"  
                elif i == "map_user":
                    create_table_query = f"CREATE TABLE {i} (state varchar(255),year varchar(255) ,reg_user varchar(255),app_opens varchar(255));"  
                if i == "top_tran":
                    create_table_query = f"CREATE TABLE {i} (state varchar(255),year varchar(255) ,entity_name varchar(255) ,type varchar(255),count varchar(255),amount varchar(255));"  
                if i == "top_user" :
                    create_table_query = f"CREATE TABLE {i} (state varchar(255),year varchar(255) ,name varchar(255), reg_user varchar(255));"  

                excute_query(connection, create_table_query)
        connection = conn.create_db_connection(db)
        return connection


# In[5]:


def update_data(database_username, database_password, database_ip, database_name):

        df_cls = get_df()
        df_cls.update_dataset()
        df_li = df_cls.get_df_li()
        conn = check_conn(database_ip, database_username, database_password, database_name)
        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
        table_list = ["agg_tran", "agg_user", "map_tran", "map_user", "top_tran", "top_user"]
        for i in range(len(df_li)):
            df_li[i].to_sql(con=database_connection, name=table_list[i], if_exists='replace')
        print("Updated the Dataset")
        return df_li


# In[6]:


database_username = 'root'
database_password = 'enter your password'
database_ip       = 'localhost'
database_name     = 'phonepe_db'

df_cls = get_df()
df_li = df_cls.get_df_li()
df_li[0]






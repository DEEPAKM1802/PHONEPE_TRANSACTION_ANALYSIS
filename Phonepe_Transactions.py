import json
import os
import re
from itertools import chain
from pathlib import Path

import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import streamlit as st
from IPython import get_ipython
from mysql.connector import Error
from plotly.offline import init_notebook_mode, iplot, plot


# will be used to store and extract data from mysql
class SQLClass:

    def __init__(self, host_name="localhost", user_name="root", user_password="1802", db_name="phonepe_guvi"):
        self.host_name = host_name
        self.user_name = user_name
        self.user_password = user_password
        self.db_name = db_name

    # ***************************************************************************************************
    def create_db_connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.host_name,
                user=self.user_name,
                password=self.user_password,
                database=self.db_name)
            print("qa_db connectionn sucessful")
        except Error as err:
            print(f"Error : '{err}'")
        return connection

    # ***************************************************************************************************
    def excute_query(self, connection, query):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
            print("Query was successful")
        except Error as err:
            print(f"Error : '{err}'")

    # ***************************************************************************************************
    def read_query(self, connection, query):
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as err:
            print(f"Error : '{err}'")

    # ***************************************************************************************************
    def get_insert_query_from_df(self, df, dest_table):

        insert = """
        INSERT INTO `{dest_table}` (
            """.format(dest_table=dest_table)

        columns_string = str(list(df.columns))[1:-1]
        columns_string = re.sub(r' ', '\n        ', columns_string)
        columns_string = re.sub(r'\'', '', columns_string)

        values_string = ''

        for row in df.itertuples(index=False, name=None):
            values_string += re.sub(r'nan', 'null', str(row))
            values_string += ',\n'

        return insert + columns_string + ')\n     VALUES\n' + values_string[:-2] + ';'


# this class is used to get all the file from git store in the same folder as our py file and create dataframes
class DataframeList:

    # ***************************************************************************************************
    def update_dataset(self):
        dir_path = os.getcwd()
        file_folder = str(dir_path) + "\\pulse"
        if os.path.isdir(file_folder) == True:
            get_ipython().system('cd die_path')
            get_ipython().system('rmdir pulse /s /q')
            print("folder del")
        print("Downloading latest dataset")
        get_ipython().system('cd dir_path')
        get_ipython().system('git clone https://github.com/PhonePe/pulse')

    # ***************************************************************************************************
    def get_all_files(self, folder):
        file_list = []
        if os.path.exists(folder):
            for root, dirs, files in os.walk(folder):
                for file in files:
                    file_list.append(os.path.join(root, file))
        return file_list

    # ***************************************************************************************************
    def create_df(self, file_li, value):
        ldata = []
        col = []
        for i in file_li:

            # read the json file
            f = open(i)
            jdata = json.load(f)
            f.close()

            # getting year and state from folder name
            state = Path(i).parts[-3]
            year = Path(i).parts[-2]
            file_name = Path(i).parts[-1]

            # Based on folder selecting data format to match json format
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
                col = ['State', 'Year', 'start', 'end', 'Name', 'Type', 'Count', 'Amount']
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
                col = ['State', 'Year', 'Quater', 'Register_Users', 'App_opens', 'Brand', 'Count', 'Percentage']

            if value == 2:
                for i in range(len(jdata["data"]["hoverDataList"])):
                    ldata.append([
                        state, year,
                        jdata["data"]["hoverDataList"][i]["name"],
                        jdata["data"]["hoverDataList"][i]["metric"][0]["type"],
                        jdata["data"]["hoverDataList"][i]["metric"][0]["count"],
                        jdata["data"]["hoverDataList"][i]["metric"][0]["amount"]
                    ])
                col = ['State', 'Year', 'Name', 'Type', 'Count', 'Amount']

            if value == 3:
                for i in jdata["data"]["hoverData"]:
                    ldata.append([
                        state, year,
                        jdata["data"]["hoverData"][i]["registeredUsers"],
                        jdata["data"]["hoverData"][i]["appOpens"]
                    ])
                col = ['State', 'Year', 'Registered_Users', 'App_Opens']

            if value == 4:
                for i in jdata["data"]:
                    if jdata["data"][i] != None:
                        for j in range(len(jdata["data"][i])):
                            ldata.append([
                                state, year,
                                jdata["data"][i][j]["entityName"],
                                jdata["data"][i][j]["metric"]["type"],
                                jdata["data"][i][j]["metric"]["count"],
                                jdata["data"][i][j]["metric"]["amount"]
                            ])
                col = ['State', 'Year', 'Entity_Name', 'Type', 'Count', 'Amount']

            if value == 5:
                for i in jdata["data"]:
                    if jdata["data"][i] != None:
                        for j in jdata["data"][i]:
                            ldata.append([
                                state, year,
                                j["name"],
                                j["registeredUsers"]
                            ])
                col = ['State', 'Year', 'Name', 'Register_User']

        # Creating a dataframe and returning it to be stored in a list
        df = pd.DataFrame(ldata, columns=col)
        return df

    # ***************************************************************************************************
    def get_df_li(self):
        dir_path = os.getcwd()
        file_folder = str(dir_path) + "\\Pulse\data"
        f1 = ["aggregated", "map", "top"]
        f2 = ["transaction", "user"]
        df_file_list = []
        for i in f1:
            for j in f2:
                temp_path = f"{file_folder}\{i}\{j}"
                df_file_list.append(self.get_all_files(temp_path))
        df_li = []
        for i in range(len(df_file_list)):
            df_li.append(self.create_df(df_file_list[i], i))

        return df_li


# The function takes data from dataframelist class and using insert query store in mysql database using class SQLClass
def fetch_store_data():
    dfobj = DataframeList()
    dflist = dfobj.get_df_li()
    sqlobj = SQLClass()
    sqlconn = sqlobj.create_db_connection()

    f1 = ["agg", "map", "top"]
    f2 = ["tra", "user"]
    count = 0
    for i in f1:
        for j in f2:
            dflist[count].replace(to_replace=[None], value="null", inplace=True)
            insert_query = sqlobj.get_insert_query_from_df(dflist[count], i + "_" + j)
            sqlobj.excute_query(sqlconn, insert_query)
            count = count + 1


# program to get data from sql for figure1
def fig1data(year):
    act_st_list = ['Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chandigarh',
                   'Chhattisgarh', 'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa', 'Gujarat', 'Haryana',
                   'Himachal Pradesh', 'Jammu & Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh',
                   'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha',
                   'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttarakhand',
                   'Uttar Pradesh', 'West Bengal']

    rd_qa = """
        SELECT * FROM agg_tra where State != 'india' and State != 'lakshadweep';
        """
    sqlobj = SQLClass()
    sqlconn = sqlobj.create_db_connection()
    rd_result = sqlobj.read_query(sqlconn, rd_qa)
    df_sql = pd.DataFrame(rd_result, columns=['State', 'Year', 'start', 'end', 'Name', 'Type', 'Count', 'Amount'])
    df = df_sql
    all_state = df.State.unique()
    all_year = df.Year.unique()
    all_name = df.Name.unique()
    agg_tra_list = []
    for i in range(len(all_state)):
        for j in all_year:
            df1 = df.query(f"State=='{all_state[i]}' and Year=='{j}'")
            df2 = df1.astype({'Amount': 'float'})
            Total = sum(df2.Amount)
            agg_tra_list.append((act_st_list[i], j, Total))
    df_con_amt = pd.DataFrame(agg_tra_list, columns=['State', 'Year', 'Amount'])

    return df_con_amt.query(f"Year=='{year}'")


# Program to plot the data from fig1data
def fig1display(year):
    df = fig1data(year)
    # Geographic Map
    figpx = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Amount',
        color_continuous_scale='Reds'
    )
    figpx.update_geos(fitbounds="locations", visible=False)
    fig = go.Figure(figpx)
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=1000,
        mapbox_center={"lat": 20.5, "lon": 78.9},
        width=800,
        height=400,
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    return fig


# program to get data from sql for figure2
def fig2data():
    rd_qa = """
        SELECT * FROM agg_tra;
        """
    sqlobj = SQLClass()
    sqlconn = sqlobj.create_db_connection()
    rd_result = sqlobj.read_query(sqlconn, rd_qa)
    df_sql = pd.DataFrame(rd_result, columns=['State', 'Year', 'start', 'end', 'Name', 'Type', 'Count', 'Amount'])
    df = df_sql
    all_state = df.State.unique()
    all_year = df.Year.unique()
    all_name = df.Name.unique()
    agg_tra_list = []
    for i in range(len(all_state)):
        for j in all_year:
            df1 = df.query(f"State=='{all_state[i]}' and Year=='{j}'")
            df2 = df1.astype({'Amount': 'float'})
            Total = sum(df2.Amount)
            agg_tra_list.append((all_state[i], j, Total))
    df_con_amt = pd.DataFrame(agg_tra_list, columns=['State', 'Year', 'Amount'])

    return df_con_amt


# plot data from fig2data
def fig2display(state):
    df = fig2data()
    df = df.query(f"State=='{state}'")
    # line plot
    fig = px.line(df, x="Year", y="Amount", title='Amount over the year')

    return fig


# streamlit function for UI
def streamlit_UI():
    st.set_page_config(page_title="Phonepe Data Analysis", layout="wide", initial_sidebar_state="collapsed")

    # Title and header to be dispalyed
    colT1, colT2 = st.columns([2.5, 5])
    with colT2:
        title = st.title(' :blue[Phonepe Data Analysis]')
    st.subheader('State wise amount over the years')

    year = st.select_slider(label="Year", options=[2018, 2019, 2020, 2021, 2022])
    fig1 = fig1display(year)
    st.plotly_chart(fig1, use_container_width=True)

    df = fig2data()
    state_li = df.State.unique()
    state = st.selectbox("State", options=state_li)
    fig2 = fig2display(state)
    st.plotly_chart(fig2, use_container_width=True)


# Main call to UI which leads to excution of entire program
maincall = streamlit_UI()

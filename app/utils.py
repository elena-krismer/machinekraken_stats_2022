import pandas as pd
import pymongo
import datetime
import streamlit as st
import plotly.express as px
import plotly

from typing import Optional
#from datetime import date, datetime

plotly.io.templates["alphastats_colors"] = plotly.graph_objects.layout.Template(
    layout=plotly.graph_objects.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        colorway=[
            "#009599",
            "#005358",
            "#772173",
            "#B65EAF",  # pink
            "#A73A00",
            "#6490C1",
            "#FF894F",
            "#2B5E8B",
            "#A87F32",
        ],
    )
)

def date_to_datetime(
    dt: datetime.date,
    hour: Optional[int] = 0,
    minute: Optional[int] = 0, 
    second: Optional[int] = 0) -> datetime.datetime:

    return datetime.datetime(dt.year, dt.month, dt.day, hour, minute, second)


def get_avg_total(df):
    total =  str(round(df['sum'].sum(), 2))
    avg = str(round(df['sum'].sum()/df.shape[0], 2))

    return total, avg


class database:
    def __init__(self, location):
        self.location = location
        self.connect_to_db()
        self.machines_list = self.get_machines_list()

    def connect_to_db(self):
        MONGODB_USER = st.secrets["MONGODB_USER"]
        MONGODB_PW = st.secrets["MONGODB_PW"]
        MONGODB_URL = st.secrets["MONGODB_URL"]
        string = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PW}@{MONGODB_URL}?ssl=true"
        client = pymongo.MongoClient(string)

        self.DB = client[self.location]
        self.queue = client[self.location]['queue']
        self.results = client[self.location]['results']

    def get_machines_list(self):
        items = pd.DataFrame(self.DB.machines.find())
        if "_id" in items.columns:
            items.drop(columns=["_id"], inplace=True)
        return items["name"].tolist()

    def get_machine(self, filename):
        for _ in self.machines_list:
            if _ in filename.lower():
                return _ 
    
    def get_acquired_data(self, start_date, end_date):
        # get list of files between start and end date
        start_date, end_date = date_to_datetime(start_date), date_to_datetime(end_date)

        items = pd.DataFrame(self.DB.files.find({
            "modified":{
                "$gte": start_date,
                "$lt": end_date
            }
        }))


        if "_id" in items.columns:
            items.drop(columns=["_id"], inplace=True)
            items = items.sort_values("modified")
            items["machine"] = items["filename"].apply(lambda x: self.get_machine(x))

            
            items['week'] = items['modified'].dt.isocalendar().week
            items['month'] = items['modified'].dt.month
            items['year'] = items['modified'].dt.isocalendar().year

        return items
    
    def get_processed_data(self, start_date, end_date):
        start_date, end_date = date_to_datetime(start_date), date_to_datetime(end_date)

        items = pd.DataFrame(self.DB.queue.find({
            "finished": {"$ne": None},
            "started":{
                "$gte": start_date,
                "$lt": end_date
            }}))
 
        items.drop(columns=["_id"], inplace=True)
        items['week'] = items['finished'].dt.isocalendar().week
        items['year'] = items['finished'].dt.isocalendar().year
        items['month'] = items['finished'].dt.month

        items["workflow_2"] = items['workflow'].str[:2]

        return items



class processed_files:
    def __init__(self, df, df_processed):
        self.df_acquired = df
        self.df_processed = df_processed

    def get_stats_machine(self, month_week):

        if month_week == "week":

            self.df_acquired['year_week'] = self.df_acquired['year'].astype('str') +"_"+ self.df_acquired['week'].apply(lambda x: f"{x:02d}")

            k = self.df_acquired[['year_week','machine','size_mb']].groupby(['year_week','machine']).sum()/1024/1000

        elif month_week == "month":
            self.df_acquired['year_month'] = self.df_acquired['year'].astype('str') +"_"+ self.df_acquired['month'].apply(lambda x: f"{x:02d}")

            k = self.df_acquired[['year_month','machine','size_mb']].groupby(['year_month','machine']).sum()/1024/1000

        k = k.unstack()
        k = k.droplevel(axis=1, level=0)

        k['sum'] = k.sum(axis=1, skipna=True)
     
            
        return k

    def get_stats_processor(self, month_week):

        if month_week == "week":

            self.df_processed['year_week'] = self.df_processed['year'].astype('str') +"_"+ self.df_processed['week'].apply(lambda x: f"{x:02d}")

            k = self.df_processed[['year_week','machine','size_gb']].groupby(['year_week','machine']).sum()/1024

        elif month_week == "month":
            self.df_processed['year_month'] = self.df_processed['year'].astype('str') +"_"+ self.df_processed['month'].apply(lambda x: f"{x:02d}")

            k = self.df_processed[['year_month','machine','size_gb']].groupby(['year_month','machine']).sum()/1024

        k = k.unstack()
        k = k.droplevel(axis=1, level=0)

        k['sum'] = k.sum(axis=1, skipna=True)
     
            
        return k

    def plot_acquired_data(self, month_week, machines):
        df = self.get_stats_machine(month_week=month_week)[machines]
        
        df.index.name = month_week
        df.reset_index(inplace=True)

        fig = px.bar(df, x=month_week, y=machines, title="Acquired data")
        fig.update_layout(legend_title="machine", yaxis_title = "Tb")

        df['sum']= df.loc[:,machines].sum(axis=1)
        total, avg = get_avg_total(df=df)
       
        return fig, total, avg


    def plot_processed_data(self, month_week):
        df = self.get_stats_processor(month_week=month_week)
        
        df.index.name = month_week
        df.reset_index(inplace=True)
     
        machines = df.columns.to_list()[ : -1]
    
        fig = px.bar(df, x=month_week, y=machines, title="Processed data")
        fig.update_layout(legend_title="machine", yaxis_title = "Tb")

        total, avg = get_avg_total(df=df)

        return fig , total, avg

    def plot_workflows(self, month_week):
        # plot jobs and workflows
        if month_week == "overview":
            workflow_count = pd.DataFrame(self.df_processed['workflow_2'].value_counts().iloc[:5])
            workflow_count = workflow_count.reset_index()
            
            fig = px.bar(workflow_count, 
                x="index", y="workflow_2",
                title = "Workflows")
            all_jobs = workflow_count
    
        else:
            workflow_count = pd.crosstab(self.df_processed[month_week],self.df_processed['workflow_2'])
            workflow_count = workflow_count.reset_index()
            workflow_count.loc[:, (workflow_count != 0).any(axis=0)]
            fig = px.bar(workflow_count, x=month_week, y=workflow_count.columns.to_list())
            fig.update_layout(legend_title="workflow")

            all_jobs = workflow_count.loc[:, workflow_count.columns != month_week].to_numpy().sum()
        
        fig.update_layout(xaxis_title = month_week, yaxis_title="count")

        return fig, all_jobs



        
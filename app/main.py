from gettext import Catalog
import os
from re import sub
from xml.dom.expatbuilder import parseFragmentString


import streamlit as st
import pandas as pd

from pprint import pprint
import datetime
import numpy as np
from utils import database, processed_files
from utils import *





class user_interface:
    def __init__(self):
        self.database = None

    def tab_main(self):
        st.markdown("# MachineKraken Statistics Overview ")

        st.markdown("## Select Location")
        location = st.selectbox('Location',('CPR', "MUC"))
        start_date = st.date_input( "Start date", datetime.datetime(2022, 1, 1))
        end_date =  st.date_input( "End date", datetime.datetime.today())
        
        self.database = database(location = location)
        df = self.database.get_data(start_date=start_date, end_date=end_date)
        df_processed = self.database.get_processed(start_date=start_date, end_date=end_date)
        pf = processed_files(df=df, df_processed = df_processed)

        self.processed_machine(pf=pf)

        self.processed_processor(pf=pf)

        self.jobs(pf=pf)
        
    
    def processed_machine(self, pf):
        st.markdown("## Processing - Machines")
        machines_to_plot = st.multiselect("Machines", self.database.machines_list, self.database.machines_list)
        month_week_1 = st.selectbox('Show',('month', "week"), key="month_week_1")
        fig_1, total, avg = pf.plot_processed_machine(month_week=month_week_1, machines=machines_to_plot)
        st.write("Total", total)
        st.write("Average", avg)
        st.plotly_chart(fig_1)
    
    def processed_processor(self, pf):
        st.markdown("## Processing - Processor")
        month_week_2= st.selectbox('Show',('month', "week"), key="month_week_2")
        fig_2, total, avg= pf.plot_processed_processor(month_week=month_week_2)
        st.plotly_chart(fig_2)
        st.write("Total", total)
        st.write("Average", avg)

    def jobs(self, pf):
        st.markdown("## Number of jobs")
        month_week_workflow= st.selectbox('Show',("overview",'month', "week"))
        fig_workflow, all_jobs= pf.plot_workflows(month_week=month_week_workflow)
        st.plotly_chart(fig_workflow)
        st.write("Total jobs: ", all_jobs)





def run():
    ui = user_interface()

    radio = st.sidebar.radio(
        "Navigation",
        options=[
            "Main"
          #
        ],
    )

    if radio == "Main":
        ui.tab_main()

    else:
        return


if __name__ == "__main__":
    run()

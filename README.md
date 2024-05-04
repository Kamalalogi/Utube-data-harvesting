
**YouTube Data Harvesting and Warehousing**

INTRODUCTION:
This project is to extract the data about Youtube channels that is channel data ,videos data of the channel's uploads playlist and its related comment datas. 

 The project utilizes SQL, Python, and Streamlit to create a user-friendly application that allows users to retrieve, store, and query YouTube channel and video data.

Key Technologies and Skills

    Python scripting
    Data Collection
    API integration
    Streamlit
    Data Management using MySQL 

Installation

To run this project, you need to install the following packages:

    pip install google-api-python-client
    from googleapiclient.errors import HttpError
    import pandas as pd
    import re
    from datetime import datetime
    from sqlalchemy import create_engine
    from sqlalchemy.exc import IntegrityError
    import streamlit as st
    import mysql.connector

Deplyment:

To use this project, follow these steps:

    Clone the repository: '''git clone https://github.com/Kamalalogi/Utube-data-harvesting/blob/main/dataharvest.py
    Install the required packages: refer installtion
    Run the Streamlit app: streamlit run app.py
    Access the app in your browser at http://localhost:8501




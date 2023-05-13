import csv
import firebase_admin
from firebase_admin import credentials, storage

import schedule
import time as tm
from datetime import datetime, timedelta, time

import pandas as pd
import numpy as np
import json
import requests
from pathlib import Path

#firebase**************************************************************************************
# Initialize Firebase Admin SDK with your service account
cred = credentials.Certificate('./service-account.json')
default_app = firebase_admin.initialize_app(cred, {
    'storageBucket': 'python-firebase-db-66ede.appspot.com'
})
bucket = storage.bucket()

def upload_to_firebase(file_name):
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name, content_type='text/csv')


def delete_from_firebase(file_name):
    blob = bucket.blob(file_name)
    blob.delete()

#process data***********************************************************************************
file_path_world_history = Path("world_history.csv")
file_path_all_country_history = Path("all_country_history.csv")

def get_world_history_data():
  #world history
  endpoint_world_history = 'https://disease.sh/v3/covid-19/historical/all?lastdays=all'

  world_history_data = requests.get(endpoint_world_history)
  data = world_history_data.json()

  #get list in dict
  world_history_list_cases = list(data['cases'].values())
  world_history_list_deaths = list(data['deaths'].values())
  world_history_list_recovered = list(data['recovered'].values())

  world_history_list_days = list(data['cases'].keys())

  #create dict
  world_history_dict_data = {
      "days": world_history_list_days,
      "cases": world_history_list_cases,
      "deaths": world_history_list_deaths,
      "recovered": world_history_list_recovered
  }

  #convert to json
  world_history_json_str = json.dumps(world_history_dict_data)

  #create dataframe
  world_history_df = pd.read_json(world_history_json_str)
  world_history_df.to_csv("world_history.csv", index=False)
  return world_history_df

def get_all_country_data():
  #all country history
  endpoint_all_country_history = 'https://disease.sh/v3/covid-19/vaccine/coverage/countries?lastdays=500'

  all_country_history_data = requests.get(endpoint_all_country_history)
  data_all_country_history = all_country_history_data.json()

  #get list day
  data_all_country_history_days = list(data_all_country_history[0]['timeline'].keys())

  #create dict
  data_all_country_history_dict = {}

  data_all_country_history_dict['days'] = data_all_country_history_days

  #add data to dict
  for item in data_all_country_history:
    country_name = item['country']
    cases = list(item['timeline'].values())
    data_all_country_history_dict[country_name] = cases

  #convert to json
  data_all_country_json_str = json.dumps(data_all_country_history_dict)

  #create dataframe
  data_all_country_df = pd.read_json(data_all_country_json_str)
  data_all_country_df.to_csv("all_country_history.csv", index=False)
  return data_all_country_df

#cronjob************************************************************************************************
def job():
    if file_path_world_history.is_file():
       file_path_world_history.unlink()

    if file_path_all_country_history.is_file():
       file_path_all_country_history.unlink()

    get_world_history_data()
    get_all_country_data()

    delete_from_firebase('world_history.csv')
    delete_from_firebase('all_country_history.csv')

    upload_to_firebase('world_history.csv')
    upload_to_firebase('all_country_history.csv')

# schedule.every(5).seconds.do(job)

schedule.every().day.at("23:00").do(job)

while True:
    schedule.run_pending()
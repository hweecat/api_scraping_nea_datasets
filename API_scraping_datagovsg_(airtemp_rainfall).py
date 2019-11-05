'''
Scraping Air Temperature and Rainfall Data from Data.gov.sg v1
Developer: Ong Chin Hwee (@hweecat), AKindSoul
Language: Python 3.7.3

This data scraping script is developed as a personal project to scrap NEA 
meteorological data from Data.gov.sg APIs. The project initiator (@hweecat) has 
active plans to expand this personal project to scrap data from other NEA Dataset 
APIs. 

Currently, this script is able to scrap data from the following APIs:
1. Realtime Weather Readings across Singapore
    a. Air Temperature across Singapore
    b. Rainfall across Singapore

This script is currently being actively updated to include scraping from other
NEA dataset APIs.
'''

import numpy as np
import pandas as pd
import datetime

import requests
import json

import pytz

from tqdm import trange
from time import sleep

import sys

def get_airtemp_data_from_date(date):
    url = "https://api.data.gov.sg/v1/environment/air-temperature?date=" + str(date) # for daily API request
    JSONContent = requests.get(url).json()
    content = json.dumps(JSONContent, indent = 4, sort_keys=True)
    json_retrieved = (content[content.find("items")+7:content.find("metadata")-13] + ']').replace(" ", "").replace("\n", "")
    df_retrieved = pd.read_json(json_retrieved, orient="columns")
    print("Data for " + str(date) + " scraped!")
    return df_retrieved

def get_rainfall_data_from_date(date):
    url = "https://api.data.gov.sg/v1/environment/rainfall?date=" + str(date) # for daily API request
    JSONContent = requests.get(url).json()
    content = json.dumps(JSONContent, indent = 4, sort_keys=True)
    json_retrieved = (content[content.find("items")+7:content.find("metadata")-13] + ']').replace(" ", "").replace("\n", "")
    df_retrieved = pd.read_json(json_retrieved, orient="columns")    
    print("Data for " + str(date) + " scraped!")
    return df_retrieved

def get_data_from_date_range(date_range, data_type):
    df_date_list = []
    for date in date_range:
        if data_type == 'air-temperature':
            df_date = get_airtemp_data_from_date(str(date))
        elif data_type == 'rainfall':
            df_date = get_rainfall_data_from_date(str(date))
        df_date_list.append(df_date)
    return pd.concat(df_date_list).reset_index()

def get_device_id(date, data_type):
    url = "https://api.data.gov.sg/v1/environment/" + data_type + "?date=" + str(date) # for daily API request
    JSONContent = requests.get(url).json()
    content = json.dumps(JSONContent, indent = 4, sort_keys=True)
    json_device_id = content[content.find("stations")+10:-3].replace(" ", "").replace("\n", "")
    df_device_id = pd.read_json(json_device_id, orient="object")
    return df_device_id

def utc_to_local(dt):
    local_tz = pytz.timezone('UTC')
    dt = local_tz.localize(dt)
    target_tz = pytz.timezone('Asia/Singapore')
    dt = target_tz.normalize(dt).replace(tzinfo=None)
    return dt

def remove_tzinfo(dt):
    return dt.replace(tzinfo=None)


'''
Initialize Input Parameters for Data Scraping
Input:
    1. Start date in YYYY-MM-DD format
    2. Number of days from start date
    3. Type of data to extract from Data.gov.sg NEA API
        - Currently supported for extraction of:
            1. Air Temperature
            2. Rainfall
    4. Device ID

'''
# Initialize date range for data extraction
try:
    date_entry = input('Enter a date in YYYY-MM-DD format: ')
    date_time_str = str(date_entry)
    try:        
        base = datetime.datetime.strptime(date_time_str, '%Y-%m-%d').date()
        if base > datetime.datetime.now().date():
            print('Date input is in the future.')
            raise ValueError
    except ValueError:
        print('Date input is not valid. Defaulting to current date.')
        base = datetime.datetime.now().date()
        date_list = [base]
    else:
        numdays_entry = input('Enter number of days from date entered: ')
        numdays = int(numdays_entry)
        try:
            date_list = [base + datetime.timedelta(days=x) for x in range(numdays)]
            if date_list[-1] > datetime.datetime.now().date():
                print('Date range goes into the future.')
                raise ValueError
        except ValueError:
            print('Date range input is not valid. Defaulting to input date.')
            date_list = [base + datetime.timedelta(days=x) for x in range(int((datetime.datetime.now().date() - base).days+1))]

    # Initialize type of data to extract from NEA data API
    datatype_entry = input('Choose type of data to extract from API - 1. air temperature 2. rainfall : ')
    datatype_choice = int(datatype_entry)
    while (datatype_choice):
        if datatype_choice == 1:
            data_type = 'air-temperature'
            break
        elif datatype_choice == 2:
            data_type = 'rainfall'
            break
        else:
            datatype_entry = input('Invalid input. Please choose type of data to extract from API - 1. air temperature 2. rainfall: ')
            datatype_choice = int(datatype_entry)

    # Extract daily data from Data.gov.sg API for a defined date range, represented in JSON format
    df_data = get_data_from_date_range(date_list, data_type)

    # Get device ID dataframe
    df_device_id = pd.concat([get_device_id(date, data_type) for date in date_list])
    # Create dictionary of station IDs for switch case to initialize station ID
    device_id_list = set(df_device_id[['device_id', 'id']].set_index('device_id')['id'])
    device_id_dict = {id:id for id in device_id_list}

    # Initialize device ID to select from extracted data
    stationid_entry = input('The station IDs are: \n' + str(list(device_id_dict.keys())) + '\nChoose station ID to extract data from: ')
    stationid_choice = str(stationid_entry)
    while True:
        stationid_choice = device_id_dict.get(stationid_choice, None)
        if stationid_choice == None:
            stationid_entry = input('Invalid station ID. Please choose station ID to extract data from: ')
            stationid_choice = str(stationid_entry)
        else:
            break

    # create list of dataframes containing extracted reading values converted from JSON format
    df_reading = []
    for reading in trange(len(df_data['readings'])):
        df_to_append = pd.DataFrame(list(df_data['readings'][reading]))

        # fill in null values for station ids without reading values for some timestamps
        for station_id in list(df_device_id['id']):
            if station_id not in list(df_to_append['station_id']):
                df_to_append = df_to_append.append(pd.DataFrame({"station_id":[station_id], "value": [np.nan]}))      
        df_to_append_null_filled = df_to_append.reset_index().drop(columns=['index']).reset_index(drop=True)
        df_reading.append(df_to_append_null_filled)

        if reading % 10 == 0:
            sleep(0.1)
        
    # concatenate dataframes in list within date range    
    df_extracted = pd.concat(df_reading)

    # extract sensor readings for a specific station id
    df_extracted_stationid = df_extracted[df_extracted['station_id']==stationid_choice].reset_index(drop=True)
    df_extracted_cleaned = pd.concat([df_data, df_extracted_stationid], axis=1).drop(columns=['readings'])

    # Convert from UTC Time to SGT Time
    if not int((pd.__version__).split('.')[1]) >= 25:
        df_extracted_cleaned['timestamp'] = [utc_to_local(dt) for dt in df_extracted_cleaned['timestamp']]

    # write to CSV
    df_extracted_cleaned.to_csv('nea_' + data_type + '_' + stationid_choice + '_from_' + str(date_list[0]) + '_to_' + str(date_list[-1]) + '.csv')
    print('Data extraction complete!')

except Exception as err:
    print(err) 
finally:
    sys.exit()
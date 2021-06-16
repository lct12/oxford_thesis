#!/usr/bin/env python
# coding: utf-8

# ### Import and Clean Data Script

# In[ ]:


import os
import gzip
import glob
import json
import pickle

import pandas as pd
import numpy as np

import osmium as osm
from os import sep


# #### Import data

# In[ ]:


# Working directory
cwd = f"C:{sep}Users{sep}ltswe{sep}Dropbox{sep}Oxford{sep}Thesis"
# Data directory is kept on flash
data_dir = "D:"


# In[ ]:


# Create list of the JSONs that will need to be loaded in 
# Glob.glob creates a list of all the files that end in .json in the directories/sub-directories of raw_tweets
# The rest of the command filters out jsons that end in 00000.json since those represent meta counts and not actual tweets
json_list = [j for j in glob.glob(f'{data_dir}{sep}raw_tweets{sep}**{sep}*.json', recursive=True)
             if j[-10:] != '00000.json']
len(json_list)


# In[ ]:


# Turn into pandas dataframe using pd.concat
# Commence with blank dataframe
raw_df = pd.DataFrame()
for j in json_list:
    temp_json = json.load(open(j))
    temp_df = pd.json_normalize(temp_json['data'])
    raw_df = pd.concat((raw_df, temp_df))


# In[ ]:


# # Pickle the raw data 
raw_tweets_pickle = open(f"{data_dir}{sep}pickle{sep}raw_tweets_df.pickle", "wb")
pickle.dump(raw_df, raw_tweets_pickle)


# In[ ]:


# Bring in pickled data
raw_df = pickle.load(open(f"{data_dir}{sep}pickle{sep}raw_tweets_df.pickle", "rb"))


# In[ ]:


raw_df


# In[ ]:


# Bring in crime and 311 data, which uses the Socrata API in NYC Open Data
# Create a function that uses the Socrata API, which is written in SoQL, a SQL-like language, to query data
from sodapy import Socrata

def socrata_API_df(source_domain, dataset_id, select_string, where_string, limit=1000):
    '''
    Inputs: 
    source_domain: This tells Socrata the source of the dataset you're querying
    dataset_id: This is the unique id of the dataset
    select_string: This string tells Socrata which variables you are selecting from the dataset
    where_string: This string is equivalent to the "where" command in SQL
    limit = This tells Socrata how many results to query. The default is 1000 b/c Socrata automatically sets it to 1000

    Outputs a dataframe with with the queried results
    '''
    keyFile = open(f'{cwd}{sep}tokens{sep}socrata_apikey.txt', 'r')
    token = keyFile.readline() #api token imported from txt file
    
    client = Socrata(source_domain, token)
    # Change timeout var to arbitrarily large # of seconds so it doesn't time out
    client.timeout = 50
    results = client.get(dataset_id, limit = limit, select = select_string, where = where_string)
    df = pd.DataFrame.from_records(results)
    return df


# In[ ]:


# Create function to shorten lat/long
# Cut lat/long to 4 decimals (10 m)
def round_lat_long(df):
    df["latitude"] = df["latitude"].apply(lambda x: round(float(x),3))
    df["longitude"]= df["longitude"].apply(lambda x: round(float(x),3))


# In[ ]:


# Pull in 311 and Null Data 
# 2007, 2008, & 2009 are separate; 2010-on are in a single file. 
# The only thing that changes between 2007-09 is the dataset ID, & the id + where string for 2010-on
# so write a function that calls upon the 311 socrata API data
# complaint type string -- separated for ease of understanding. Complaint types drawn from literature
complaint_type_str = "complaint_type = 'Noise - Street/Sidewalk' OR complaint_type = 'Noise - Residential' OR complaint_type = 'Noise - Vehicle' OR complaint_type = 'Street Condition' "                     "OR complaint_type = 'Homeless Encampment' OR complaint_type = 'Drinking' OR complaint_type = 'Noise' "                     "OR complaint_type = 'Noise - Park' OR complaint_type = 'Noise - House of Worship' OR complaint_type = 'HEATING' "                     "OR complaint_type = 'GENERAL CONSTRUCTION' OR complaint_type = 'CONSTRUCTION' OR complaint_type = 'Boilers' "                     "OR complaint_type = 'For Hire Vehicle Complaint' OR complaint_type = 'Bike Rack Condition' OR complaint_type = 'Illegal Parking' "                     "OR complaint_type = 'Building/Use' OR complaint_type = 'ELECTRIC' OR complaint_type = 'PLUMBING'"

def pull_311(dataset_id, where_string = f'latitude IS NOT NULL AND ({complaint_type_str})'):
    return socrata_API_df(source_domain = "data.cityofnewyork.us", dataset_id = dataset_id,                          select_string = 'unique_key, created_date, complaint_type, date_extract_y(created_date) as year, date_extract_m(created_date) as month, descriptor, latitude, longitude',                          where_string = where_string,
                         limit = 4000000)

# 2007-2013
nyc_311_07 = pull_311("aiww-p3af")
nyc_311_08 = pull_311('uzcy-9puk')
nyc_311_09 = pull_311('3rfa-3xsf')
nyc_311_10_13 = pull_311('erm2-nwe9',                 where_string = f'({complaint_type_str}) AND latitude IS NOT NULL AND (year = 2010 OR year = 2011 OR year = 2012 OR year = 2013)')

# Combine all four
nyc_311 = nyc_311_07.append(nyc_311_08).append(nyc_311_09).append(nyc_311_10_13)


# In[ ]:


nyc_311.complaint_type.unique()


# In[ ]:


nyc_311_pickle = open(f"{data_dir}{sep}pickle{sep}nyc_311.pickle", "wb")
pickle.dump(nyc_311, nyc_311_pickle)


# In[ ]:


# Pull in NYC historical crime data (also uses Socrata data)
select_string = 'cmplnt_num, cmplnt_fr_dt AS date, date_extract_y(cmplnt_fr_dt) AS year,'     'date_extract_m(cmplnt_fr_dt) AS month,  pd_cd AS class, pd_desc, law_cat_cd AS level, crm_atpt_cptd_cd AS completed, latitude, longitude'
where_string = 'latitude IS NOT NULL AND (year = 2007 OR year = 2008 OR year = 2009 OR year = 2010 OR year = 2011 OR year = 2012 OR year = 2013)'
nyc_crime = socrata_API_df(source_domain = "data.cityofnewyork.us", dataset_id = 'qgea-i56i',                            select_string = select_string, where_string = where_string, limit = 4000000)


# In[ ]:


nyc_crime_pickle = open(f"{data_dir}{sep}pickle{sep}nyc_crime.pickle", "wb")
pickle.dump(nyc_crime, nyc_crime_pickle)


# In[ ]:


# Get latitude/longitude for data
import requests
import json

def geocode_api(lat, long):
    '''Given a latitude and longitude, this function uses the FCC geocoding API to return the corresponding census tract FIPS code'''
    resp = requests.get("https://geo.fcc.gov/api/census/block/find?latitude=" + str(lat) + "&longitude=" + str(long) + "&format=json")
    j = resp.json() #turn to JSON
    fips = j['Block']["FIPS"] #get census tract number from JSON
    return fips

def geocode_df(df):
    '''Given a dataframe with two cols with latitude and longitude coordinates, this function uses the geocode_api function to 
    return a list that equals the census tract FIPS code for those lat/long coords'''
    fips_list = []
    lat_col = df.columns.get_loc("latitude")
    long_col = df.columns.get_loc("longitude")
    for row in range(df.shape[0]): #run through every row of the data frame
        lat = df.iloc[row, lat_col] #get latitude coordinate for this listing
        long = df.iloc[row, long_col] #get longitude coor for this listing
        fips = geocode_api(lat, long) #get fips code for listing
        fips_list.append(fips)
    return fips_list


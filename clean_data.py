#!/usr/bin/env python
# coding: utf-8

# ### Import and Clean Data Script

# In[67]:


import os
import gzip
import glob
import json
import pickle

import geopandas as gp
import pandas as pd
import numpy as np

import osmium as osm
from os import sep


# #### Import data

# In[2]:


# Working directory
cwd = f"C:{sep}Users{sep}ltswe{sep}Dropbox{sep}Oxford{sep}Thesis"
# Data directory is kept on flash
data_dir = "D:"


# In[172]:


# Restructure JSONs into JSOLs (where each line = one tweet)
json_list = [j for j in glob.glob(f'{data_dir}{sep}raw_tweets{sep}**{sep}*.json', recursive=True)
             if j[-10:] != '00000.json']
test_json = json_list[2]
temp_json = json.load(open(test_json))
# Turn JSON into JSOl where key =  by using json['data']
# then allocate to census tract using shape files & multiprocessing (one core for each month-year)


# In[176]:


temp_json2 = temp_json['data']


# In[188]:


test_dict = {temp_json2[i]['id']:temp_json2[i] for i in range(len(temp_json2))}
# test_dict


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


# In[82]:


# Bring in HUD vacant addresses data
# Create list of the excel files that will need to be loaded in 
# Glob.glob creates a list of all the files that end in .xlsx in the directory of HUD vacant data
# The rest of the command filters out jsons that end in 00000.json since those represent meta counts and not actual tweets
hud_list = [j for j in glob.glob(f'{data_dir}{sep}hud_vacant_data{sep}*.csv')]


# In[117]:


hud_df = pd.DataFrame()
for file in hud_list:
    temp_file = pd.read_csv(file, sep = None, engine='python')
#   Using title of the file, create a column for the year & month/quarter
    temp_file['year'] = ["20"+file[32:34] for i in range(temp_file.shape[0])]
    temp_file['month'] = [file[28:30] for i in range(temp_file.shape[0])]
    hud_df = hud_df.append(temp_file).reset_index(drop=True)


# In[165]:


# Create a FIPS code variable that is equal to the string of geoid 
# (note that b/c it's in integers, we need to re-add leading zero for states with fips codes < 10) 
hud_df['fips_code'] = hud_df["GEOID"].apply(lambda x: ("0" + str(x))[-11:])

# Create file with only NY 
ny_hud = hud_df[hud_df['fips_code'].apply(lambda x: x[0:2] == "36")].reset_index(drop=True)

# Pickle ny hug file
nyc_hud_pickle = open(f"{data_dir}{sep}pickle{sep}nyc_hud.pickle", "wb")
pickle.dump(ny_hud, nyc_hud_pickle)


# In[166]:


# Unpickle ny hud
ny_hud = pickle.load(open(f"{data_dir}{sep}pickle{sep}nyc_hud.pickle", "rb"))


# In[167]:


ny_hud


# In[68]:


temp_shp = gp.read_file(f"{data_dir}{sep}test{sep}tl_rd13_36001_edges.shp")


# In[70]:


temp_shp['geometry']


# In[ ]:





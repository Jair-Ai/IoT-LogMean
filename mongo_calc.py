#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 08:52:17 2019

@author: jair_rapids
"""
#All imports needed
import pymongo
from pymongo import DESCENDING
import pandas as pd
import numpy as np
import datetime as dt
import time

#Server connect data
mongo_user = 'root'
mongo_password = 'pass'
mongo_db = "test_IoT2"
port_to_connect = 27017

#Colection DB from iot data
collection = 'hospitais'

#Colection DB to store calc data
collection2 = 'calc_data'


#Definifition times in epoch
time_15min = 900.0
time_30min = 1800.0
time_1hour = 3600.0
time_4hour = 14400.0
time_8hour = 28800.0
time_12hour = 43200.0
time_1day = 86400.0


#column for calc
coluna = 'date'

#connect to Mongo
client = pymongo.MongoClient()

client = pymongo.MongoClient('localhost', 27017)

#Connect to db
mydb = client[mongo_db]


#Connect to collection to find
posts = mydb.hospitais

#Connect To collection to insert

posts2 = mydb.metricas

# Function to take the last register
def get_last(coluna):

    """
    @param coluna: Time epoch column to start range
    """
    with client:
        
        db = client.test_IoT2
        
        ultimo = list(db.hospitais.find().sort("_id", DESCENDING).limit(1))
        
        a = ultimo[0]
        c = a[coluna]
    return c


def calculations(df, column):
    """
    @param df: Dataframe from IoT db
    @param column: column chosen to make the calculations
    """
    
    new_float = df[column].astype(float)
    max_value = new_float.max()
    min_value = new_float.min()
    log_means = log_mean(new_float)
    
    return max_value, min_value, log_means



#Function with looping and the last find, making the calculation
    
def log_mean(tes):

    """
    @param tes: Array to make logmean
    """

    mean_log = tes/10
    mean_log = np.power(10, mean_log)
    total = mean_log.sum()
    media = total / len(mean_log)
    resultado_final = np.log10(media)
    resultado_final = resultado_final*10

    return resultado_final   


#Function to use when time call it, on heartbeat

def all_stuffs(time, dt, end):
    """
    @param time: Time chosen to take range, ex: 5min, 15min, in Epoch
    @dt: Datetime to save on server
    @param end: last record that will be taken in the db, in epoch
    """
    start = int((end-time))
    with client:
        db = client.test_IoT2
        data_filtered = list(db.hospitais.find({'date': {'$gte': start}}))
    df = pd.DataFrame(data_filtered, columns=['date', 'p', 'h', 'd', 'f'])
    maxi, mini, logm = calculations(df, coluna)
    dict_to_insert = {'date': dt,
                      'max': maxi,
                      'min': mini,
                      'logm': logm}
    result = posts2.insert_one(dict_to_insert)
    

    
def heartbeat():

   
    while True:
        
        currenttime = dt.datetime.now()
        if currenttime.second == 0 and currenttime.min % 15 == 0:
            end = get_last(coluna)
            all_stuffs(time_15min, currenttime, end)
            time.sleep(240)
        
        if (currenttime.minute == 30 or 00) and currenttime.second == 0:
            all_stuffs(time_30min, currenttime, end)

        if currenttime.minute == 00 and currenttime.second == 0:
            all_stuffs(time_1hour, currenttime, end)
            
        if currenttime.hour % 4 == 0 and currenttime.minute == 00 and currenttime.second == 0:
            all_stuffs(time_4hour, currenttime, end)
            
        if currenttime.hour % 8 == 0 and currenttime.minute == 00 and currenttime.second == 0:
            all_stuffs(time_8hour, currenttime, end)
            
        if (currenttime.hour % 12 == 0 or currenttime.hour ==0) and currenttime.minute == 00 and currenttime.second == 0:
            all_stuffs(time_12hour, currenttime, end)
            
        if currenttime.hour == 0 and currenttime.minute == 00 and currenttime.second == 0:
            all_stuffs(time_1day, currenttime, end)
    
         
#Finaly run the function
heartbeat()
#testing multiple git account


from statsmodels.tsa.arima_model import ARIMA
import pandas as pd
import pmdarima as pm
import numpy as np
from pymongo import MongoClient
import logging
import requests
import json
import os

def prediccion_arima(n_periods):
    logging.info('Prediccion del algoritmo ARIMA') 
    
    client=MongoClient('mongodb://mongodb:27017/')
    db_collection = client.meteosat_db.predicciones
    # return  db_collection.to_dict('dict')
    # df = pd.DataFrame(list(db_collection.find()))

    df = pd.DataFrame(list(db_collection.find().limit(1000)))
    # del df['_id']
    # return df.to_string()
    # print("Error al conectar con la BD")

    modelTemp = pm.auto_arima(df['TEMP'], start_p=1, start_q=1,
                      test='adf',       # use adftest to find optimal 'd'
                      max_p=3, max_q=3, # maximum p and q
                      m=1,              # frequency of series
                      d=None,           # let model determine 'd'
                      seasonal=False,   # No Seasonality
                      start_P=0, 
                      D=0, 
                      trace=True,
                      error_action='ignore',  
                      suppress_warnings=True, 
                      stepwise=True)

    modelHum = pm.auto_arima(df['HUM'], start_p=1, start_q=1,
                        test='adf',       # use adftest to find optimal 'd'
                        max_p=3, max_q=3, # maximum p and q
                        m=1,              # frequency of series
                        d=None,           # let model determine 'd'
                        seasonal=False,   # No Seasonality
                        start_P=0, 
                        D=0, 
                        trace=True,
                        error_action='ignore',  
                        suppress_warnings=True, 
                        stepwise=True)

    fcTemp, confint = modelTemp.predict(n_periods=n_periods, return_conf_int=True)
    temperatura=np.array(fcTemp)

    fcHum, confint = modelHum.predict(n_periods=n_periods, return_conf_int=True)
    humedad=np.array(fcHum)

    return [{'hours':str(i%24).zfill(2)+':00',"temp":round(temperatura[i], 2),"hum":round(humedad[i], 2)} for i in range(0, n_periods)]
    # return ['{"hour":"00:00","temp":25,"hum":15}']


def prediccion_weatherstack(n_periods):
    API_KEY = os.environ.get("API_KEY")
    
    params = {
      'key': API_KEY,
      'city': 'Granada',
      'country': 'ES',
      'hours':n_periods
    }
    api_result = requests.get('https://api.weatherbit.io/v2.0/forecast/hourly', params)
    # api_result = requests.get('https://api.weatherbit.io/v2.0/forecast/hourly?city=Seville,ES&key='+API_KEY+'&hours=120')


    api_response = api_result.json()

    return [{'hours':str(i['datetime'])[len(i['datetime'])-2:]+':00',"temp":i['temp'],"hum":i['rh']} for i in api_response['data']]
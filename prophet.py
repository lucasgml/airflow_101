import gzip
import shutil
import requests
import os
from os import listdir
from os.path import isfile,join,abspath

from datetime import timedelta

import pandas as pd
from fbprophet import Prophet

url = 'https://data.brasil.io/dataset/covid19/caso_full.csv.gz'

def download_data():
    r = requests.get(url, allow_redirects=True)
    open('../data/data.csv.gz', 'wb').write(r.content)

def unzip_data():
    with gzip.open('../data/data.csv.gz', 'rb') as f_in:
        with open('../data/data.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def delete_file():
    os.remove('../data/data.csv.gz')
    os.remove('../data/data.csv')
    shutil.rmtree('../data/figs')
    
def create_fig_folder():
    try:
        os.mkdir('../data/figs')
    except:
        print('LOG - Folder exists')

#Generating predictions per state    
def read_data_state(state):
    data = pd.read_csv('../data/data.csv')
    state_bool = data['state'] == state
    cities_bool = pd.notna(data['city'])
    state_data = data[state_bool & cities_bool]
    grouped_data = state_data.groupby('date') \
                   .agg({'last_available_confirmed':'sum'})
    grouped_data = grouped_data.reset_index()
    grouped_data.columns = ['ds','y']
    
    model = Prophet()
    model.fit(grouped_data)

    future = model.make_future_dataframe(periods=15)
    forecast = model.predict(future)
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()

    fig1 = model.plot(forecast)
    fig1.savefig(f'../data/figs/state_{state}',dpi=400)

#Generating predictions per city
def read_data_city(city):
    data = pd.read_csv('../data/data.csv')
    city_data = data[data['city'] == city]
    grouped_data = city_data.groupby('date') \
                   .agg({'last_available_confirmed':'sum'})
    grouped_data = grouped_data.reset_index()
    grouped_data.columns = ['ds','y']
    
    model = Prophet()
    model.fit(grouped_data)

    future = model.make_future_dataframe(periods=15)
    forecast = model.predict(future)
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()

    fig1 = model.plot(forecast)
    fig1.savefig(f'../data/figs/city_{city}',dpi=400)
    
def generate_graphs():
    for city in ['Fortaleza','São Paulo']:
        read_data_city(city)
    for state in ['CE','SP']:
        read_data_state(state)

def read_files(folder='../data/figs/'):
    list_of_files = [abspath(folder+f) for f in listdir(folder) if isfile(join(folder, f))]
    return list_of_files
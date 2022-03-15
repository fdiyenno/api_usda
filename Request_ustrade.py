# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 13:09:49 2020

@author: Fede

Status codes
The request we just made had a status code of 200. Status codes are returned with every request that is made to a web server. Status codes indicate information about what happened with a request. Here are some codes that are relevant to GET requests:

200 — everything went okay, and the result has been returned (if any)
301 — the server is redirecting you to a different endpoint. This can happen when a company switches domain names, or an endpoint name is changed.
401 — the server thinks you’re not authenticated. This happens when you don’t send the right credentials to access an API (we’ll talk about authentication in a later post).
400 — the server thinks you made a bad request. This can happen when you don’t send along the right data, among other things.
403 — the resource you’re trying to access is forbidden — you don’t have the right permissions to see it.
404 — the resource you tried to access wasn’t found on the server.

https://www.dataquest.io/blog/python-api-tutorial/
https://realpython.com/python-requests/
https://www.programiz.com/python-programming/json
https://requests.readthedocs.io/en/master/

"""
# Tutorial Request
# https://www.census.gov/data/developers/data-sets/international-trade.html

# Página de USATRADE
# USER ID: 82TF7H3
# PASS ID: eV7iY6TWmMbTu#9

# https://usatrade.census.gov/index.php
# https://api.census.gov/data/timeseries/intltrade/exports/porths/variables.html

import requests
import pandas as pd
import json

# URLS
porths_url = "https://api.census.gov/data/timeseries/intltrade/exports/porths"
porths = "https://api.census.gov/data/timeseries/intltrade/exports/porths.json"
catalog ='https://project-open-data.cio.gov/v1.1/schema/catalog.json'
geography = 'https://api.census.gov/data/timeseries/intltrade/exports/porths/geography.json'
variables = "https://api.census.gov/data/timeseries/intltrade/exports/porths/variables.json"
example = 'https://api.census.gov/data/timeseries/intltrade/exports/porths/examples.json'
groups = 'https://api.census.gov/data/timeseries/intltrade/exports/porths/groups.json'
values = 'https://api.census.gov/data/timeseries/intltrade/exports/porths/values.json'

#print(help(porths_url))
response0 = porths_url
response = requests.get(response0)
response.status_code
response.json()
response.headers
# See the content type of the response payload.
response.headers['Content-Type']

#Grabo las variables en una tabla de pandas
response0 = variables
response = requests.get(response0)
response.json()
alpha = response.content
variables = pd.read_json(alpha)

# Requests values


# Commodities trabajo Rosario-NewOrleans-Santos
productos = pd.read_excel('productos_complejos.xlsx',header=0,converters={'NCM':str})
productos.dtypes # tipos de datos dentro de las columnas 
productos['NCM_6']=productos['NCM'].str.slice(stop=6) # split it based on the first characters
commodity = pd.Series(productos['NCM_6'].values) #transformo el dataset en "series"
commodity = set(commodity) # transformo las series en un "set"

response = requests.get(porths_url,
                        params= {'get': ['PORT_NAME,VES_WGT_MO,PORT,MONTH'],
                                 #'datetime': {'year': 'True', 'month': 'True'},
                                 #'time': {'from1997-01to2012-01'},
                                 'YEAR': {'2019','2018',"2017","2016"},
                                 #'YEAR': {'2018'},
                                 #'MONTH': {''},
                                 #'PORT': {'2002'},
                                 'COMM_LVL': {'HS6'},
                                 'E_COMMODITY': commodity,
                                 }
                        )
response.url           
response.status_code
#print(response)
response.json()
alpha = response.content


import pandas as pd

df = pd.read_json(alpha)
new_header = df.iloc[0] #grab the first row for the header
df = df[1:] #take the data less the header row
df.columns = new_header #set the header row as the df header
df[["VES_WGT_MO"]] = df[["VES_WGT_MO"]].apply(pd.to_numeric) # convert just columns "a" and "b"
#df[["a", "b"]] = df[["a", "b"]].apply(pd.to_numeric) # convert just columns "a" and "b"

de = df.groupby(["PORT_NAME", "PORT", "YEAR","E_COMMODITY"])["VES_WGT_MO"].sum()/1000
de = de.to_frame()
de = de.reset_index()

pd.options.display.float_format = '{:,}'.format
de.reset_index(drop=False, inplace=True)
print('\nResult :\n', de) 
de.dtypes

de.to_excel('exportaciones_eeuu.xlsx', index=False, sheet_name='exportaciones_eeuu')



# Requests puertos
response = requests.get(porths_url,
                        params= {'get': ['PORT_NAME'],
                                 #'datetime': {'year': 'True', 'month': 'True'},
                                 #'time': {'from1997-01to2012-01'},
                                 #'YEAR': {'2019','2018',"2017","2016"},
                                 #'YEAR': {'2018'},
                                 #'MONTH': {''},
                                 'MONTH': {''},
                                 'YEAR': {'2019','2018',"2017","2016"},
                                 'PORT': {''},
                                 #'PORT_NAME': {''},
                                 #'COMM_LVL': {'HS4'},
                                 #'E_COMMODITY': {'1201'}
                                 }
                        )
response.url           
response.status_code
#print(response)
response.json()
alpha = response.content


df_1 = pd.read_json(alpha)
new_header = df_1.iloc[0] #grab the first row for the header
df_1 = df_1[1:] #take the data less the header row
df_1.columns = new_header #set the header row as the df header

duplicados = df_1.pivot_table(index=['PORT_NAME'], aggfunc='size') #contando cuantos duplicados hay en una columna
print(duplicados)
# sorting by first name 
df_1.sort_values("PORT_NAME", inplace = True)
# dropping ALL duplicte values 
df_1.drop_duplicates(subset ="PORT_NAME", 
                     keep = "first", inplace = True) 
df_1.reset_index(drop=True, inplace=True)
df_1 = df_1. iloc[:, 0] # get first column of `df`
df_1

# Requests productos
response = requests.get(porths_url,
                        params= {'get': ['E_COMMODITY_LDESC'],
                                 #'datetime': {'year': 'True', 'month': 'True'},
                                 #'time': {'from1997-01to2012-01'},
                                 #'YEAR': {'2019','2018',"2017","2016"},
                                 #'YEAR': {'2018'},
                                 #'MONTH': {''},
                                 #'MONTH': {''},
                                 'YEAR': {'2019'},
                                 'COMM_LVL': {'HS6'},
                                 'E_COMMODITY': {''}
                                 #'COMM_LVL': {'HS4'},
                                 #'E_COMMODITY': {'1201'}
                                 }
                        )
response.url           
response.status_code
#print(response)
response.json()
alpha = response.content


df_2 = pd.read_json(alpha)
new_header = df_2.iloc[0] #grab the first row for the header
df_2 = df_2[1:] #take the data less the header row
df_2.columns = new_header #set the header row as the df header

df_2.sort_values('E_COMMODITY', inplace = True) # sorting by column
# dropping ALL duplicte values 
df_2.drop_duplicates(subset ='E_COMMODITY_LDESC', 
                     keep = "first", inplace = True)
df_2.reset_index(drop=True, inplace=True)
df_2.sort_values('E_COMMODITY', inplace = True)

df_2 = df_2. iloc[:, 0] # get first column of `df`


reload = productos_1.to_dict('records')
print(reload)
print(thisdict)

productos[0].to_dict('series')

commodity = productos['NCM_6'].tolist()
comm = {commodity}























# Guardo el contenido en un archivo json
with open('outputfile.json', 'wb') as outf:
    outf.write(response.content)

import json

with open(tradeus_c) as f:
  data = json.load(tradeus_c)

if response.status_code == 200:
    print('Success!')
elif response.status_code == 404:
    print('Not Found.')




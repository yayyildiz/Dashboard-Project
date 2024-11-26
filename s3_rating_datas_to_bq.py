# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 14:50:45 2024

@author: DTYYILDIZ
"""

import requests
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import gzip

def download_file(signed_url, gz_path):
    
    response = requests.get(signed_url)
    if response.status_code == 200:
        with open(gz_path, 'wb') as file:
            file.write(response.content)
        print("File downloaded successfully")
    else:
        print(f"Failed to download file, status code: {response.status_code}")

def func(gz_path):
    
    with gzip.open(gz_path, 'rt', encoding='utf-8') as file:
        df = pd.read_csv(file, encoding='utf-8')

    
    df['session tags'] = df['session tags'].astype(str)
    
    
    df_split = df['session tags'].str.split('&').apply(
        lambda x: {item.split('=')[0]: item.split('=')[1] if '=' in item else None for item in x}
    )
    
    
    df_split_df = pd.DataFrame(df_split.tolist())
    df = pd.concat([df.drop(columns=['session tags']), df_split_df], axis=1)
   
   
    print(df.columns)
    
    return df

def clean_column_names(df):
    
    
    df.columns = df.columns.str.replace('[^0-9a-zA-Z_]', '_', regex=True)
    df.columns = df.columns.str.replace('^[^a-zA-Z_]', '_', regex=True)
   
    
    df.columns = df.columns.str.replace('%C4%B1', u"\u0131")  # ı harfi
    df.columns = df.columns.str.replace('%C3%A7', u"\u00E7")  # ç harfi
    df.columns = df.columns.str.replace('%C4%9F', u"\u011F")  # ğ harfi
    df.columns = df.columns.str.replace('%C3%B6', u"\u00F6")  # ö harfi
    df.columns = df.columns.str.replace('%C5%9F', u"\u015F")  # ş harfi
    df.columns = df.columns.str.replace('%C3%BC', u"\u00FC")  # ü harfi

    df.columns = df.columns.str.replace('%C3%87', u"\u00C7")  # Ç harfi
    df.columns = df.columns.str.replace('%C4%B0', u"\u0130")  # İ harfi
    df.columns = df.columns.str.replace('%C4%9E', u"\u011E")  # Ğ harfi
    df.columns = df.columns.str.replace('%C3%96', u"\u00D6")  # Ö harfi
    df.columns = df.columns.str.replace('%C5%9E', u"\u015E")  # Ş harfi
    df.columns = df.columns.str.replace('%C3%9C', u"\u00DC")  # Ü harfi
    
    return df

def clean_column_values(df):

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.replace('%C4%B1', u"\u0131")  # ı harfi
        df[col] = df[col].str.replace('%C3%A7', u"\u00E7")  # ç harfi
        df[col] = df[col].str.replace('%C4%9F', u"\u011F")  # ğ harfi
        df[col] = df[col].str.replace('%C3%B6', u"\u00F6")  # ö harfi
        df[col] = df[col].str.replace('%C5%9F', u"\u015F")  # ş harfi
        df[col] = df[col].str.replace('%C3%BC', u"\u00FC")  # ü harfi

        df[col] = df[col].str.replace('%C3%87', u"\u00C7")  # Ç harfi
        df[col] = df[col].str.replace('%C4%B0', u"\u0130")  # İ harfi
        df[col] = df[col].str.replace('%C4%9E', u"\u011E")  # Ğ harfi
        df[col] = df[col].str.replace('%C3%96', u"\u00D6")  # Ö harfi
        df[col] = df[col].str.replace('%C5%9E', u"\u015E")  # Ş harfi
        df[col] = df[col].str.replace('%C3%9C', u"\u00DC")  # Ü harfi
        
        
        df[col] = df[col].str.replace('%2F', u"\u002F")     # / karakteri
        df[col] = df[col].str.replace('+', ' ')             # + karakteri
        df[col] = df[col].str.replace('%3A', u"\u003A")     # : karakteri
        df[col] = df[col].str.replace('%5B', u"\u005B")     # [ karakteri
        df[col] = df[col].str.replace('%5D', u"\u005D")     # ] karakteri
        df[col] = df[col].str.replace('%3B', u"\u003B")     # ; karakteri
        df[col] = df[col].str.replace('%28', u"\u0028")     # ( karakteri
        df[col] = df[col].str.replace('%29', u"\u0029")     # ) karakteri
        df[col] = df[col].str.replace('%2C', u"\u002C")     # , karakteri

    return df

def create_bq_table(df, credentials, project_id, table_id):
    
   
    df = clean_column_names(df)
    df = clean_column_values(df)

    
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, credentials=credentials, if_exists='replace')

def main():
   
    now = datetime.now().date()
    yesterday = now - timedelta(days=1)
    gz_path = f'C:/Users/DTYYILDIZ/Desktop/DailySessionLog_Digiturk_{yesterday}.csv.gz'
    json_path = 'C:/Users/DTYYILDIZ/Desktop/XXXXX.json'
    project_id = 'XXXXX'
    table_id = 'XXXXX'
  
   
    signed_url = f"https://XXXXX.net/DailySessionLog_Digiturk_{yesterday}.csv.gz"
    
    
    download_file(signed_url, gz_path)
    
    
    df = func(gz_path)
    print(df.head())  
    
   
    credentials = service_account.Credentials.from_service_account_file(json_path)
    create_bq_table(df, credentials, project_id, table_id)


main()


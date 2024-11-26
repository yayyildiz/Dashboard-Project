# -*- coding: utf-8 -*-
"""
Created on Mon Aug 12 13:45:58 2024

@author: DTYYILDIZ
"""

import requests
import pandas as pd
from pandas import json_normalize
from google.cloud import bigquery
from google.oauth2 import service_account

def create_bq_table(df_fixture, credentials, project_id, table_id):
  
    client = bigquery.Client(credentials=credentials, project=project_id)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df_fixture, table_id, job_config=job_config)
    job.result()  
    print(f"Data loaded into {table_id}.")

def get_token(auth_url, username, password):
   
    payload = {'grant_type': 'password', 'username': username, 'password': password}
    try:
        response = requests.post(auth_url, data=payload)
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.exceptions.RequestException as e:
        print(f"Token could not be retrieved: {e}")
        return None

def get_fixture(token, url):
    
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json().get('data')
    except requests.exceptions.RequestException as e:
        print(f"Data could not be retrieved: {e}")
        return None

def process_fixtures(all_fixtures):
  
    final_df = pd.concat(all_fixtures, ignore_index=True)
    id_columns = [col for col in final_df.columns if 'id' in col.lower()]
    final_df[id_columns] = final_df[id_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
    return final_df

def main():
    auth_url = 'XXXXXX'
    username = 'XXXXX'
    password = 'XXXXX'
    
    
    organization_ids = ['10', '11', '12', '17', '18', '393', '130', '88']
    season_ids = ['3566', '3565', '3562', '3542', '3580', '3613', '3583', '3561']

    
    token = get_token(auth_url, username, password)
    if not token:
        print("Token could not be retrieved.")
        return  

    all_fixtures = []
   
    for organization_id in organization_ids:
        for season_id in season_ids:
            fixture_url = f'https://XXXXX/api/fixture/organizationid/{organization_id}/seasonid/{season_id}'
            data = get_fixture(token, fixture_url)
            if data:
                print(f"Data retrieved: Organization ID {organization_id}, Season ID {season_id}")
                df_fixture = json_normalize(data, sep='_')
                all_fixtures.append(df_fixture)
            else:
                print(f"No data retrieved: Organization ID {organization_id}, Season ID {season_id}")

    
    if all_fixtures:
        final_df = process_fixtures(all_fixtures)
        print("Final DataFrame:")
        print(final_df)

        
        json_path = 'C:/Users/DTYYILDIZ/Desktop/XXXXXX.json'
        project_id = 'XXXXXX'
        table_id = 'XXXXXX'
        credentials = service_account.Credentials.from_service_account_file(json_path)
        
        
        create_bq_table(final_df, credentials, project_id, table_id)
    else:
        print("No data retrieved.")


main()















  

    

    
        
   
    
  
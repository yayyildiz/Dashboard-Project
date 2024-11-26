import cx_Oracle
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def func():
    
    username = 'XXX'
    password = ''
    host = 'XXXX'
    port = 'XXXX'
    service_name = 'XXXXX'

    
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    
    
    connection = cx_Oracle.connect(username, password, dsn)
    cursor = connection.cursor()
    
    try:
       
        query = """SELECT DISTINCT CMS_CONTENT_ID, NAME, OPTA_ID, START_TIME
                   FROM (SELECT t.CMS_CONTENT_ID,
                                t.NAME,
                                t.OPTA_ID,
                                t.START_TIME
                           FROM AAT.MATCH_FIXTURE_DEV t
                          WHERE t.OPTA_ID IS NOT NULL
                          UNION ALL
                         SELECT t.CMS_CONTENT_ID,
                                t.NAME,
                                t.OPTA_ID,
                                t.START_TIME
                           FROM AAT.MATCH_FIXTURE t)"""
        
        cursor.execute(query)
        
        
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=columns)
        
       
        df['NAME'] = df['NAME'].str.upper()
        
        
        df['START_TIME'] = df['START_TIME'].astype(str)
        df['START_TIME'] = df['START_TIME'].str.replace('T', ' ').str.replace('Z', ' ')
        
       
        df['CMS_CONTENT_ID'] = df['CMS_CONTENT_ID'].astype(str)

        
        print(df.info())
        
    finally:
       
        cursor.close()
        connection.close()
    
    return df


def write_append(df):
    
    credentials = service_account.Credentials.from_service_account_file("C:/Users/DTYYILDIZ/Desktop/XXXXX.json")
    client = bigquery.Client(credentials=credentials, project='XXXXX')
    
    
    df.reset_index(drop=True, inplace=True)
    
    
    schema = [
        bigquery.SchemaField("CMS_CONTENT_ID", "STRING"),
        bigquery.SchemaField("NAME", "STRING"),
        bigquery.SchemaField("OPTA_ID", "STRING"),
        bigquery.SchemaField("START_TIME", "STRING"),
    ]
    
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND" 
    )
    
    
    job = client.load_table_from_dataframe(df, 'XXXXX', job_config=job_config)
    job.result()  


def main():
    
    df = func()
    
    
    print(df.head())
    
    
    write_append(df)


main()


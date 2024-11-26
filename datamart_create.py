import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/DTYYILDIZ/Desktop/XXXXXX.json"
client = bigquery.Client()

project_id = 'XXXXX'
dataset_id = 'XXXX'


SCHEMA = [
    bigquery.SchemaField("CMS_CONTENT_ID", "STRING"),
    bigquery.SchemaField("NAME", "STRING"),
    bigquery.SchemaField("START_TIME", "TIMESTAMP"),
    bigquery.SchemaField("season_name", "STRING"),
    bigquery.SchemaField("round", "INTEGER"), 
    bigquery.SchemaField("score_homeScore", "STRING"),
    bigquery.SchemaField("score_awayScore", "STRING"),
    bigquery.SchemaField("matchDate", "DATE"),
    bigquery.SchemaField("league_name", "STRING"),
    bigquery.SchemaField("userType", "STRING"),
    bigquery.SchemaField("playing_time__ms_", "INTEGER"),
    bigquery.SchemaField("viewerId", "STRING")
]


def create_or_update_table():
    table_id = f"{project_id}.{dataset_id}.project"
    table = bigquery.Table(table_id, schema=SCHEMA)
    table = client.create_table(table, exists_ok=True)


def run_query_to_dataframe():
    query = """
    SELECT 
        oracle.CMS_CONTENT_ID,
        oracle.NAME,
        oracle.START_TIME,
        opta.season_name,
        opta.round,
        opta.score_homeScore,
        opta.score_awayScore,
        opta.matchDate,
        opta.league_name,
        test_izleme_verisi.userType,
        test_izleme_verisi.playing_time__ms_,
        test_izleme_verisi.viewerId
        
    FROM 
        `XXXXX.XXX.oracle_fixture` AS oracle
    JOIN 
        `XXXX.XXX.opta_verileri` AS opta
    ON 
        oracle.OPTA_ID = CAST(opta.id AS STRING)
    JOIN
        `XXXX.Conviva.session_source_data_all` AS test_izleme_verisi
    ON
        CAST(oracle.CMS_CONTENT_ID AS STRING) = CAST(test_izleme_verisi.c3CmId AS STRING)
    WHERE 
        test_izleme_verisi.date >= "2024-08-09"
    """
    
    df = pandas_gbq.read_gbq(query, project_id=project_id, credentials=client._credentials)
    
    
    df['START_TIME'] = pd.to_datetime(df['START_TIME'], errors='coerce') 
    
    
    df['round'] = pd.to_numeric(df['round'], errors='coerce').fillna(0).astype('int64')
    
    
    df['score_homeScore'] = df['score_homeScore'].astype(str)
    df['score_awayScore'] = df['score_awayScore'].astype(str)
    
   
    df['matchDate'] = pd.to_datetime(df['matchDate'], errors='coerce').dt.date  
    
    return df


def create_bq_table(df, credentials, project_id, table_id):
    client = bigquery.Client(credentials=credentials, project=project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=SCHEMA,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ]
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

def main():
    create_or_update_table()
    
    json_path = 'C:/Users/DTYYILDIZ/Desktop/XXXXX.json'
    credentials = service_account.Credentials.from_service_account_file(json_path)
    table_id = 'XXXXX'
    
    
    df = run_query_to_dataframe()  
    
    
    create_bq_table(df, credentials, project_id, table_id)

main()




from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email_smtp
import pandas as pd
import json
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import StringIO
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator

time_format = '%Y-%m-%d %H:%M:%S'


header_file = '/home/ubuntu/airflow/dags/header.json'

with open(header_file, 'r') as file:
    header = json.load(file)

def extract(ti):
    url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'
    parameters = {
        'slug': 'bitcoin',
        'convert': 'USD'
    }
    
    headers = header

    session = Session()
    session.headers.update(headers)
    try:
        response = session.get(url, params=parameters)
        response.raise_for_status()
        data = json.loads(response.text)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
        raise AirflowException(f"Failed to fetch data: {e}")
    
    pd.set_option('display.max_columns', None)
    df = pd.json_normalize(data['data'])
    ti.xcom_push(key='extracted_data', value=df.to_json())

def transform(ti):
    try:
        df_json = ti.xcom_pull(key='extracted_data', task_ids='extract_data_task')
        if df_json is None:
            raise ValueError("No data available from the extract task")
        
        df = pd.read_json(StringIO(df_json))

        # Remove '1.' prefix from column names
        df.columns = df.columns.str.replace('1.', '')
        
        # Drop unnecessary columns
        columns_to_drop = ['id','tags', 'infinite_supply', 'platform', 'cmc_rank', 'is_fiat',
                           'self_reported_circulating_supply', 'self_reported_market_cap',
                           'tvl_ratio', 'quote.USD.tvl']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
        
        # Convert numeric columns to appropriate types and formats
        columns_to_convert = ['quote.USD.volume_24h', 'quote.USD.market_cap', 'quote.USD.fully_diluted_market_cap']
        for col in columns_to_convert:
            df[col] = df[col].apply(lambda x: '{:.0f}'.format(x))

        columns_to_rename = {
            'quote.USD.price': 'quote_USD_price',
            'quote.USD.volume_24h': 'quote_USD_volume_24h',
            'quote.USD.volume_change_24h': 'quote_USD_volume_change_24h',
            'quote.USD.percent_change_1h': 'quote_USD_percent_change_1h',
            'quote.USD.percent_change_24h': 'quote_USD_percent_change_24h',
            'quote.USD.percent_change_7d': 'quote_USD_percent_change_7d',
            'quote.USD.percent_change_30d': 'quote_USD_percent_change_30d',
            'quote.USD.percent_change_60d': 'quote_USD_percent_change_60d',
            'quote.USD.percent_change_90d': 'quote_USD_percent_change_90d',
            'quote.USD.market_cap': 'quote_USD_market_cap',
            'quote.USD.market_cap_dominance': 'quote_USD_market_cap_dominance',
            'quote.USD.fully_diluted_market_cap': 'quote_USD_fully_diluted_market_cap',
            'quote.USD.last_updated': 'quote_USD_last_updated'
        }

        df.rename(columns=columns_to_rename, inplace=True)
        
        # Convert date columns to datetime 
        columns_to_convert_datatypes = ['date_added', 'last_updated', 'quote_USD_last_updated']
        for col in columns_to_convert_datatypes:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        filename = '/home/ubuntu/Crypto_Data.csv'
        df.to_csv(filename, index=False)
        
        ti.xcom_push(key='transformed_data', value=df.to_json())
        return df  
    
    except Exception as e:
        raise AirflowException(f"Error in transform task: {e}")

def load(ti):
    try:
        hook = PostgresHook(postgres_conn_id='crypto_db_connection')
        hook.copy_expert(
            sql="""
            COPY cryptocurrencies(name, symbol, slug, num_market_pairs, date_added, max_supply, 
                                  circulating_supply, total_supply, is_active, last_updated, 
                                  quote_usd_price, quote_usd_volume_24h, quote_usd_volume_change_24h, 
                                  quote_usd_percent_change_1h, quote_usd_percent_change_24h, 
                                  quote_usd_percent_change_7d, quote_usd_percent_change_30d, 
                                  quote_usd_percent_change_60d, quote_usd_percent_change_90d, 
                                  quote_usd_market_cap, quote_usd_market_cap_dominance, 
                                  quote_usd_fully_diluted_market_cap, quote_usd_last_updated) 
            FROM stdin WITH DELIMITER as ',' CSV HEADER
            """,
            filename='/home/ubuntu/Crypto_Data.csv'
        )
        print("Data loaded successfully into the database")
    except Exception as e:
        raise AirflowException(f"Error in load task: {e}")


default_args = {
    'owner': 'Chidera',
    'email': ['fegidorenaissanceclassof18@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

crypto_dag = DAG(
    'crypto_dag',
    default_args=default_args,
    schedule_interval='0 0 1 * *',
    start_date=datetime(2024, 6, 21),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract,
    dag=crypto_dag
)

transform_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform,
    dag=crypto_dag
)

file_sensor_task = FileSensor(
    task_id='file_sensor',
    filepath='/home/ubuntu/Crypto_Data.csv',
    poke_interval=300,
    timeout=600,
    dag=crypto_dag
)

create_table_task = PostgresOperator(
    task_id='create_table_task',
    postgres_conn_id='crypto_db_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS cryptocurrencies (
        result_id SERIAL PRIMARY KEY, 
        name VARCHAR(255),
        symbol VARCHAR(10),
        slug VARCHAR(255),
        num_market_pairs INT,
        date_added TIMESTAMP,
        max_supply BIGINT,
        circulating_supply BIGINT,
        total_supply BIGINT,
        is_active BOOLEAN,
        last_updated TIMESTAMP,
        quote_usd_price DECIMAL(20, 10),
        quote_usd_volume_24h DECIMAL(30, 0),
        quote_usd_volume_change_24h DECIMAL(20, 10),
        quote_usd_percent_change_1h DECIMAL(20, 10),
        quote_usd_percent_change_24h DECIMAL(20, 10),
        quote_usd_percent_change_7d DECIMAL(20, 10),
        quote_usd_percent_change_30d DECIMAL(20, 10),
        quote_usd_percent_change_60d DECIMAL(20, 10),
        quote_usd_percent_change_90d DECIMAL(20, 10),
        quote_usd_market_cap DECIMAL(20, 0),
        quote_usd_market_cap_dominance DECIMAL(20, 10),
        quote_usd_fully_diluted_market_cap DECIMAL(20, 0),
        quote_usd_last_updated TIMESTAMP
    );
    """,
    dag=crypto_dag
)

load_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load,
    dag=crypto_dag
)

email_task = EmailOperator(
    task_id='email_task',
    to=['chideraozigbo@gmail.com'],
    subject='Crypto Data CSV',
    html_content="""
    <p>Hello Team,</p>
    <p>I trust you are doing great today.</p>
    <p>Find attached in this email is the latest Crypto Data that was loaded into our Database from our crypto pipeline.</p>
    <p>Best Wishes.</p>
    """,
    files=['/home/ubuntu/Crypto_Data.csv'],
    dag=crypto_dag
)


extract_task >> transform_task >> create_table_task >> file_sensor_task >> load_task >> email_task

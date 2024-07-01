from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pandas as pd
import json
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import StringIO

time_format = '%Y-%m-%d %H:%M:%S'

def extract(ti):
    url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'
    parameters = {
        'slug': 'bitcoin',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': '9af647f6-55fa-4193-93db-6e69f1af611a',
    }
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
        columns_to_drop = ['tags', 'infinite_supply', 'platform', 'cmc_rank', 'is_fiat',
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
        
        # Convert date columns to datetime format
        columns_to_convert_datatypes = ['date_added', 'last_updated', 'quote_USD_last_updated']
        for col in columns_to_convert_datatypes:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        ti.xcom_push(key='transformed_data', value=df.to_json())
        return df  
    
    except Exception as e:
        raise AirflowException(f"Error in transform task: {e}")

def load(ti):
    try:
        df_json = ti.xcom_pull(key='transformed_data', task_ids='transform_data_task')
        if df_json is None:
            raise ValueError("No data available from the transform task")
        
        df = pd.read_json(StringIO(df_json))

        pg_hook = PostgresHook(postgres_conn_id='crypto_db_connection')
        engine = pg_hook.get_sqlalchemy_engine()
        
        try:
            # Attempt to load data into the PostgreSQL database
            df.to_sql('cryptocurrencies', engine, if_exists='replace', index=False)
            print("Data loaded successfully into the database")

        except Exception as db_exception:
            # Atempts to load the data into a csv
            filename = f"airflow/Bitcoin-Crypto-ETL-Data-Pipeline/dags/Files/Crypto_data_as_at_{datetime.now().strftime(time_format)}.csv"
            df.to_csv(filename, index=False)
            print(f"Data loaded successfully into CSV file: {filename}")

    except Exception as e:
        raise AirflowException(f"Error in load task: {e}")

default_args = {
    'owner': 'Chidera',
    'email': ['chideraozigbo@gmail.com', 'femi.eddy@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(seconds=20)
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

load_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load,
    dag=crypto_dag
)

extract_task >> transform_task >> load_task

from datetime import timedelta
import math
from stringprep import b3_exceptions

from elasticsearch import RequestError
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['julien.godfroy27@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(days=5),
}

def execute_dbt_command(dbt_command):
    task_id = dbt_command.replace(' ', '_')
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd /home/julien/dbt_BigDataProject/YoutubeBigData && {dbt_command}",
        dag=dag
    )


def load_to_elastic():



    import pandas as pd
    from elasticsearch import Elasticsearch
    import psycopg2

   
    
    def preprocess_document(document):
    # Handle NaN values in the document
        if math.isnan(document['likeCount']):
            # Replace NaN with a default value
            document['likeCount'] = 0
        
        if math.isnan(document['commentCount']):
            document['commentCount'] = 0
        return document
    


    # PostgreSQL connection details
    connection_string = "postgresql://postgres:postgres@riendutout.juniorisep.com:54333/postgres"
    
    # Schema and table names
    schema_name = "godfroy_dutilleul"
    table_name = "marts_youtube_data"
    
    # Define the mapping
    mapping = {
        "properties": {
            "title": {"type": "text"},  # Change the mapping type to "text"
            "description": {"type": "text"},
            "publishedAt": {"type": "date"},
            "viewCount": {"type": "long"},
            "likeCount": {"type": "long"},
            "commentCount": {"type": "integer"},
            "video_id": {"type": "keyword"},
            "channel_rank": {"type": "integer"},
            "channel_name": {"type": "text"},
            "channel_link": {"type": "keyword"},
            "channel_id": {"type": "keyword"},
            "channel_uploads": {"type": "long"},
            "channel_subs": {"type": "long"},
            "channel_totalviews": {"type": "long"},
            "channel_categ": {"type": "text"}
        }
    }



    

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(connection_string)
    
    # SQL query to select data from the table
    query = f"SELECT * FROM {schema_name}.{table_name};"
    
    # Fetch data from the table into a pandas DataFrame
    df = pd.read_sql_query(query, conn)
    
    # Close the database connection
    conn.close()

    # Connexion à Elasticsearch
    es = Elasticsearch(['http://localhost:9200'])
    # Nom de l'index Elasticsearch
    index_name = 'video_data'

    try:
        # Delete the index if it already exists
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            print(f"Deleted existing index: {index_name}")

        # Create the index with the new mapping
        es.indices.create(index=index_name, body={"mappings": mapping})
        print(f"Created index: {index_name}")

        # ...

    except b3_exceptions.ConnectionError as e:
        print(f"Failed to connect to Elasticsearch: {e}")
        raise

    except Exception as e:
        print(f"Failed to load data to Elasticsearch: {e}")
        raise


    # Parcours des lignes du DataFrame
    for _, row in df.iterrows():
        # Conversion de la ligne en dict pour l'importation dans Elasticsearch
        document = preprocess_document(row.to_dict())

        try:
        # Index the document
            es.index(index=index_name, body=document)
        except RequestError as e:
            print(f"Failed to index document: {document}")
            print(f"Error message: {e}")
            raise e

    # Fermeture de la connexion à Elasticsearch
    es.close()


with DAG(
    'testdag_elas',
    default_args=default_args,
    description='A simple dbt pipeline',
    schedule_interval='@monthly',
) as dag:

    # Define your tasks
    t0 = PythonOperator(
        task_id='run_python_script',
        python_callable=load_to_elastic,
        dag = dag
    )

  

    # Define your pipeline
    t0 


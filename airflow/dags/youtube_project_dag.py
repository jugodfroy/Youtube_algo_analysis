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


# Python scripts

def scrap_data():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from bs4 import BeautifulSoup, SoupStrainer

    # Chemin vers le webdriver de Selenium (ex: chromedriver pour Chrome)
    DRIVER_PATH = 'path/to/webdriver'

    # URL de la page à scraper
    URL = 'https://socialblade.com/youtube/top/country/fr/mostsubscribed'

    # Configuration de Selenium avec les options du navigateur
    chrome_options = Options()
    #chrome_options.add_argument("--headless")  # Exécution en arrière-plan, sans fenêtre du navigateur
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(executable_path=DRIVER_PATH, options=chrome_options)

    # Charger la page dans le navigateur
    driver.get(URL)

    # Attendre que la page se charge complètement
    driver.implicitly_wait(10)

    # Extraire le contenu HTML de la page chargée
    html_content = driver.page_source

    # Fermer le navigateur
    driver.quit()

    # Récupérer uniquement les balises 'a' (en gros les liens) du html de la page avec BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser', parse_only=SoupStrainer('a'))
    print(soup.prettify())




    #   [markdown]
    # Etape 2 : Depuis le contenu scrappé, Réupérer dans un dataframe les élément suivants : 
    # - Le rang de la chaine
    # - Le nom de la chaine
    # - Le lien vers la chaine
    # - L'ID de la chaine

    #  
    import pandas as pd

    #create a list to store the dictionaries representing each row
    rows = []

    #for youtube channel links and channel ids and channel names
    top_channels_id_list = []
    rank = 1
    for link in soup.find_all('a'):
        if isinstance(link.get('href'), str) and "youtube/channel" in link.get('href'):
            channel_id = link.get('href').split('/')[3]
            channel_link = link.get('href')
            channel_name = link.string

            new_row = {'rank': rank, 'channel_name': channel_name, 'channel_link': channel_link,
                    'channel_id': channel_id, 'uploads': None, 'subs': None, 'views': None, 'category': None}

            rows.append(new_row)
            rank += 1
            top_channels_id_list.append(channel_id)

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(rows)

    df.head()




    #   [markdown]
    # On recommence le scrapping, mais cette fois ci on focus les balide "i" qui contiennent la catégorie de la chaine sous forme d'icone

    #  
    #On this webpage, for each channel, an icone is displayed to indicate the category of the channel. This icon is identifiable with : <i style="color:#aaa; padding-left: 5px;" title="Category: entertainment" class="fa fa-hand-spock-o" aria-hidden="false"></i>

    #We will use this icon to extract the category of each channel
    soup = BeautifulSoup(html_content, 'html.parser', parse_only=SoupStrainer('i'))

    #Create a list of categories
    top_channels_category_list = []
    for link  in soup.find_all('i'):
        if isinstance(link.get('title'), str) and link.get('title').startswith('Category: '):
            #print (link.get('title'))
            top_channels_category_list.append(link.get('title').split(':')[1].strip())

    print(top_channels_category_list)
    #add this list to the dataframe
    df['category'] = top_channels_category_list
    df.head()


    #   [markdown]
    # Etape 3 : Maintenant qu'on a récupéré les id des chaines du TOP, on peut récupérer les données de chaque chaine en utilisant l'API Youtube (pour avoir des données plus à jour que celles sur socialblade) :
    # - Nombre d'abonnés
    # - Nombre de vidéos
    # - Nombre de vues total

    #  
    # Import Module
    from googleapiclient.discovery import build

    # Create YouTube Object
    youtube_jisep = build('youtube', 'v3',
                    developerKey='AIzaSyBYk2kRzOl4QY-kSubvLwRIeOt-kFMfVtA')
    youtube_private = build('youtube', 'v3',
                    developerKey='AIzaSyDTFdhvtxpdmRocpPCfxjMaHgYemOz4qgc')

    #for each channel id, get the uploads, subs and views with youtube API

    for index, row in df.iterrows():
        if index % 2 == 0:
            youtube = youtube_jisep
        else:
            youtube = youtube_private
        request = youtube.channels().list(
            part="statistics",
            id=row['channel_id']
        )
        response = request.execute()
        #print(response)
        df.loc[index, 'uploads'] = response['items'][0]['statistics']['videoCount']
        df.loc[index, 'subs'] = response['items'][0]['statistics']['subscriberCount']
        df.loc[index, 'views'] = response['items'][0]['statistics']['viewCount']    

    print(df.head())


    import psycopg2
    import pandas as pd
    from sqlalchemy import create_engine



    #connect to the database
    engine = create_engine('postgresql://postgres:postgres@riendutout.juniorisep.com:54333/postgres')
    conn = engine.connect()

    #write the 2 tabels to the database in godfroy_dutilleul schema
    df.to_sql('landing_channels_ranking', engine, schema='godfroy_dutilleul', if_exists='replace', index=False)

    print("UPLOADED TO POSTGRESQL")

    #  
    #save df to csv
    #df.to_csv('channels_ranking.csv', index=False)

def get_data_api():
    # ## Part 2 : Création de la deuxième Table : Stats des 25 dernières vidéos de chaque chaine du TOP 100

    #  
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine
    df_video = pd.DataFrame(columns = ['channel_id', 'title', 'description', 'publishedAt', 'viewCount', 'likeCount', 'commentCount'])


    #  
    def get_data_from_channel(channel_id, data_list, youtube):
        """Get the data of the 25 last videos of a channel"""

        # create a list to store the dictionaries representing each row
        rows = []

        # get data on the 25 last videos of this channel
        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            maxResults=25,
            order="date",
            type="video"
        )
        response = request.execute()

        # for each video, get the data of the video:
        
        for item in response['items']:
            
            video_id = item['id']['videoId']
            request = youtube.videos().list(
                part="statistics",
                id=video_id
            )
            response = request.execute()

            # store in a dictionary the data of the 25 last videos of this channel
            video_data = {
                'channel_id': channel_id,
                'video_id': item['id']['videoId'],
                'title': item['snippet']['title'],
                'description': item['snippet']['description'],
                'publishedAt': item['snippet']['publishedAt'],
                'viewCount': None,
                'likeCount': None,
                'commentCount': None
            }

            # Check if viewCount is present in the response
            if 'viewCount' in response['items'][0]['statistics']:
                video_data['viewCount'] = response['items'][0]['statistics']['viewCount']

            # Check if likeCount is present in the response
            if 'likeCount' in response['items'][0]['statistics']:
                video_data['likeCount'] = response['items'][0]['statistics']['likeCount']

            # Check if commentCount is present in the response
            if 'commentCount' in response['items'][0]['statistics']:
                video_data['commentCount'] = response['items'][0]['statistics']['commentCount']

            # append the video data dictionary to the list
            rows.append(video_data)



        # append the list of video data dictionaries to the main data list
        data_list.extend(rows)

        return data_list

    # create an empty list to store the data dictionaries
    data_list = []


    ## GET THE LIST OF ALL YOUTUBE CHANNEL IDs
    # PostgreSQL connection details
    connection_string = "postgresql://postgres:postgres@riendutout.juniorisep.com:54333/postgres"

    # Schema and table names
    schema_name = "godfroy_dutilleul"
    table_name = "landing_channels_ranking"

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(connection_string)

    # SQL query to select data from the table
    query = f"SELECT * FROM {schema_name}.{table_name};"

    # Fetch data from the table into a pandas DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    top_channels_id_list = df['channel_id'].tolist()
    print(top_channels_id_list)
    print(len(top_channels_id_list))

    #  
    # Create YouTube Object
    from googleapiclient.discovery import build


    youtube_jisep = build('youtube', 'v3',
                    developerKey='AIzaSyBYk2kRzOl4QY-kSubvLwRIeOt-kFMfVtA')
    youtube_private = build('youtube', 'v3',
                    developerKey='AIzaSyDTFdhvtxpdmRocpPCfxjMaHgYemOz4qgc')


    # iterate through the top_channels_id_list and call the function
    i = 0
    for channel_id in top_channels_id_list:
        print(channel_id)
        if i % 2 ==0:
            youtube = youtube_jisep
        else:
            youtube = youtube_private
        data_list = get_data_from_channel(channel_id, data_list, youtube)
        i += 1

    # convert the list of dictionaries to a DataFrame
    df_video = pd.DataFrame(data_list)

    df_video.head()

    #  
    #save to csv
    #df_video.to_csv('videos_stats.csv', index=False)

    #  
    #write df and df_video to a postgre database
    import psycopg2
    import pandas as pd
    from sqlalchemy import create_engine



    #connect to the database
    engine = create_engine('postgresql://postgres:postgres@riendutout.juniorisep.com:54333/postgres')
    conn = engine.connect()

    #write the table to the database in godfroy_dutilleul schema
    df_video.to_sql('landing_video_stats', engine, schema='godfroy_dutilleul', if_exists='replace', index=False)

def load_to_elastic_local():
    """NOT USED ANYMORE : USE load_to_elastic_cloud instead"""


    import pandas as pd
    from elasticsearch import Elasticsearch
    import psycopg2

    import pandas as pd
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

def load_to_elastic_cloud():
    """This function is used to load the data to a cloud elastic search instance"""


    import pandas as pd
    from elasticsearch import Elasticsearch, RequestError, TransportError
    import psycopg2

    import pandas as pd
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
    from elasticsearch import Elasticsearch

    # Password for the 'elastic' user generated by Elasticsearch
    ELASTIC_PASSWORD = "jE2W24Jnr45W8hChysytefQi"

    # Found in the 'Manage Deployment' page
    CLOUD_ID = "4d63986d06f54b0988bc2a028175de69:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ5ZTEwYjJkY2NjODE0NGU0YTFkYTY0MDA5ZDAxYTE5YiQwMzBjOTJiYzIyMDk0NDI4ODNiNmQzYjQwNzNkMzY5Zg=="

    # Create the client instance
    es = Elasticsearch(
        cloud_id=CLOUD_ID,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )





    # Nom de l'index Elasticsearch
    index_name = 'videos_data_v2'

    try:
        # Delete the index if it already exists
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            print(f"Deleted existing index: {index_name}")

        # Create the index with the new mapping
        es.indices.create(index=index_name, body={"mappings": mapping})
        print(f"Created index: {index_name}")

    except TransportError as e:
        print(f"Failed to connect to Elasticsearch: {e}")
        raise e

    except Exception as e:
        print(f"Failed to load data to Elasticsearch: {e}")
        raise e


    # Parcours des lignes du DataFrame
    for _, row in df.iterrows():
        # Conversion de la ligne en dict pour l'importation dans Elasticsearch
        document = preprocess_document(row.to_dict())

        try:
        # Index the document
            es.index(index=index_name, body=document)
            print(f"Indexed document: {document} -> done")
        except RequestError as e:
            print(f"Failed to index document: {document}")
            print(f"Error message: {e}")
            raise e

    # Fermeture de la connexion à Elasticsearch
    es.close()


with DAG(
    'youtube_dag',
    default_args=default_args,
    description='A simple dbt pipeline',
    schedule_interval='@monthly',
) as dag:

    # Define your tasks
    t0 = PythonOperator(
        task_id='scrap_data_to_get_the_ranking',
        python_callable=scrap_data
    )

    t1 = PythonOperator(
        task_id='get_videos_data_from_youtube_api',
        python_callable=get_data_api
    )


    t2 = execute_dbt_command('dbt run')

    t3 = PythonOperator(
        task_id='import_from_postgres_to_elastic',
        python_callable=load_to_elastic_cloud
    )

    t4 = execute_dbt_command('dbt docs generate')


    t5 = BashOperator(
        task_id='dbt_serve_docs_with_timeout',
        bash_command='cd /home/julien/dbt_BigDataProject/YoutubeBigData && timeout 200 dbt docs serve --port 8888 || true'
    )


    # Define your pipeline
    t0 >> t1 >> t2 >> t3 >> t4 >> t5



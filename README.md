
# YouTube Algorithm Analysis

## Introduction

This project, part of a larger BigData initiative, aims to dissect and understand the mechanics behind YouTube's recommendation algorithm. Specifically, we explore the characteristics that make a YouTube channel popular. Our approach involves dynamic data acquisition from various sources, focusing on YouTube's top channels and their video statistics.


### Subject
This study revolves around analyzing the factors that contribute to a YouTube channel's popularity. We leverage data from:
- The top 100 French YouTube channels by subscriber count (obtained via web scraping).
- Statistics for the latest 25 videos from each channel in the top 100 (sourced through YouTube API).

### Technologies Used
We employed several tools and technologies, including:
- Airflow (Local)
- Elasticsearch (Online)
- Kibana (Online)
- PostgreSQL (Online)
- Dbt (Local)
- Python for web scraping (BeautifulSoup4)

## Data Ingestion

Our data sources include:
1. **Top 100 French YouTube Channels**: Scraped from [socialblade.com](socialblade.com/youtube/top/country/fr/mostsubscribed), including details like channel name, ID, link, category, number of followers, and total views.
2. **Statistics for the Latest 25 Videos**: Data fetched using YouTube API, including video title, description, publication date, views, comments, and likes.

## Transformation with DBT

DBT (Data Build Tool) plays a crucial role in our data transformation process. We use it to transform and model data, preparing it for analysis. The transformed data is stored in a PostgreSQL database under the name `marts_youtube_data`, adhering to DBT naming conventions.

## Indexation and Visualization

Data is indexed to Elasticsearch using a Python script and visualized using Kibana. Our dashboard provides insights into the dataset and its underlying patterns.

## Automation with Airflow

The entire data processing chain is automated using Airflow. Our DAG includes tasks for scraping data, running DBT transformations, importing data to Elasticsearch, and generating DBT documentation.




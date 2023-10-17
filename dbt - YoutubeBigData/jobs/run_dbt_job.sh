#!/bin/bash
cd /home/julien/dbt_BigDataProject/YoutubeBigData/
dbt run &&
dbt docs generate &&
dbt docs serve --port 8888

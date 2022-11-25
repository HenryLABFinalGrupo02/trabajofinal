# ETL Automation

  

## Overview

There are two stages in the ETL process. The first one is the initial load where the database structure is defined and the second stage is the handling of incremental loads.

The data is captured from a JSON. The collected data is cleaned and processed through the use of scripts and is ultimately stored in Cassandra.

Jobs are written to Spark and the task flow is scheduled in Airflow every 30 seconds, through the DAG files.

  

![](https://lh5.googleusercontent.com/GM-VUfapG4wFGURUhefJES1Uu4P8XmaL4xVo1A9lnURQtvy7TMAueEyQ0a0LVFJHOD6TPrd0QGj-aW3Ir5KGsvkw1J6nW-IhkQW9CFRznEGslcmicOf3dar0fxahpXZUBJJiTO5qZdRSN7ZgwH9Qc6DZooWz5TzH8h5IlANY769YwjbcqkdsRWwwxChbLQ)

  

### DAG’s functions

The directed acyclic graph has the function of including the automated ETL Flow instructions. In our project we configure two files called `dag_initial_loading.py` and `functions.py` that include the instructions of the first stage of the ETL, which consist of extracting the data, transforming it and loading it in the Data WareHouse and the file `dags.py` includes the instructions of the second stage of the ETL that consist of the incremental loading of the data.

  

## Initial ETL Flow

1.  The data is in 5 JSON files.
    
2.  The data is moved to a work zone configured in Docker using Airflow.
    
3.  The data is imported as a pyspark pandas dataframe.
    
4.  Transformations are applied to the data to make it conform to the format required by the Cassandra database.
    
5.  The data is loaded into Cassandra using the Cassandra-Driver library.
    

  

## Environment Setup

Docker Container Recommended Specifications

memory=16GB # Limits VM memory in WSL 2

processors=4 # Makes the WSL 2 VM use 4 virtual processors

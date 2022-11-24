<<<<<<< HEAD
![FantansyLogo](https://d31uz8lwfmyn8g.cloudfront.net/Assets/logo-henry-white-lg.png)

# **Henry Final Project - Group Nº2**

- - -

# <h1 align="center">**`YELP REVIEWS AND RECOMMENDATIONS`**</h1>

<p align="center">
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/a/ad/Yelp_Logo.svg/2560px-Yelp_Logo.svg.png"  height="100">
</p>


## **Context**

"Yelp is a platform for reviews of all types of businesses, restaurants, hotels, services, among others. Users use the service and then upload their review based on the experience they have received. This information is very valuable for companies, since it gives them It serves to find out the image that users have of the different company premises, being useful to measure the performance, usefulness of the premises, in addition to knowing in which aspects the service must be improved."


## **Working proposal**

"As part of a data consultancy, we have been hired to perform an analysis of the US market. Our client is part of a conglomerate of restaurant and related companies, and they want to have a detailed analysis of the opinion of the users on Yelp about hotels, restaurants and other businesses related to tourism and leisure, using sentiment analysis, to predict which business items will grow (or decline) the most.In addition, they want to know where it is convenient to locate the new restaurants and related premises, and They want to be able to have a restaurant recommendation system for Yelp users to give the user the ability to be able to learn about new flavors based on their previous experiences.They can change the type of business (doesn't have to be restaurants).
"

You can read the full instructions [here](https://github.com/soyHenry/PF_DS/blob/main/Proyectos/Yelp.md)

- - -
## **Team**

| **Role** | **Name** | **Github** |
|:---:|:---:|---|
| _Data Analyst_ | Lila Alves | @LilaAlvesDC |
| _Data Engineer_ | David Duarte | @acidminded95 |
| _Data Engineer_ | Julieta Ciare | @julieta77 |
| _Data Manager_ | Thiago Ferster | @CodeKova |
| _Data Scientist_ | Maico Bernal | @maicobernal |

- - -
## **Data structure**
You can read the full data structure analysis [here]("/DOCS/data_structure.md")



- - -
## **Planification**

### **First Week**

Focus mainly on data exploration, tech stack to implement with emphasis on data flow and machine learning engineering for making predictions which can generate value for the client. 
You can watch the slides presentation [here](https://www.canva.com/design/DAFSIRysJ-c/zHkvCn-BxRCWirkRg5Mugw/view?utm_content=DAFSIRysJ-c&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton)



### **Second Week**

Data engineering.
You can see specific documentation [here]("./DOCS/week2.md")

### **Third Week**
Data analysis and machine learning.
You can see specific process documentation for Data Analysis [here]()
You can see specific process documentation for Machine Learning [here]()

### **Last Week**
Fine tuning of bugs and deployment. You can see the full deploy [here]()


## ** Video 
You can see the full demo from the project [here]()
=======

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

>>>>>>> 51d43e908be3e8025d49aed63481b3d49972dc03

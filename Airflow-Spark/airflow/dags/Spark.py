import pandas as pd
import os
import numpy as np
import sqlalchemy
import chardet
import sqlalchemy
import pathlib
#import datefinder
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.sql.functions import isnan, when, count, col

os.environ["JAVA_HOME"] = "/opt/java"
os.environ["SPARK_HOME"] = "/opt/spark"

findspark.init("/opt/spark")




#def drop_table(table_name, engine): #Funcion para borrar una tabla
#    Base = declarative_base()
#    metadata = MetaData()
#    metadata.reflect(bind=engine)
#    table = metadata.tables[table_name]
#    if table is not None:
#        Base.metadata.drop_all(engine, [table], checkfirst=True)
#
#db_engine = sqlalchemy.create_engine('sqlite:////opt/airflow/data/Carga/DB/DataBase.db')




def listar_archivos(): #Funcion para listar los archivos de Precios
    archivos_precio = []
    c = pathlib.Path(r'/opt/airflow/data/Carga/Salida/')
    
    for entrada in c.iterdir():
        if entrada.is_file():
            archivos_precio.append(entrada)
    
    archivos_precio.sort()
    print(pd.read_csv(archivos_precio[0]))
    print(str(archivos_precio[0]),'hola')
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.load(str(archivos_precio[0]), format="csv")
    
    print('hola2')
    print(df.show(truncate=False))
    print('hola3')


#def leer_precios(**context): #Funcion para leer los archivos de precios listados
#    archivos_precio = context['task_instance'].xcom_pull(task_ids='Listar_Precios')
#    precios_dicc = {} #Definimos un diccionario
#    #Iteramos sobre los archivos de la carpeta precios
#    for file_path in archivos_precio:
#    
#        print('******************************\n DEBUG 1 \n ******************************')
#        #Definimos si el archivo es CSV o TXT
#        if(str(file_path.suffix) in ['.csv','.txt']):
#
#            
#            print('******************************\n DEBUG 2 \n ******************************')
#            #Detectamos el Encoding
#            with open(file_path, 'rb') as f:
#                enc = chardet.detect(f.read())
#                
#            #Abrimos el archivo
#            with open(file_path,'rb') as file:
#            
#                print('******************************\n DEBUG 3 \n ******************************')
#                #Detectamos si usar | como separador
#                if( str(file.readline()).__contains__('|') ):
#                    print('******************************\n DEBUG 4 \n ******************************')
#                    #Definimos el DataFrame con su separador y encoding
#                    df = pd.read_csv( file_path , sep='|', encoding=enc['encoding'])
#    
#                    #Normalizamos
#                    df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
#                    df['precio'] = df['precio'].apply(lambda x: float(x) if(type(x) != str) else np.NaN)
#                    df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
#                    df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
#    
#                    #Lo asignamos con su fecha en el diccionario
#                    for date in datefinder.find_dates(str(file_path)):
#                        df['actualizado'] = date.strftime('%Y-%m-%d')
#                        precios_dicc[date.strftime('%Y-%m-%d')] = df
#                else:
#                    print('******************************\n DEBUG 5 \n ******************************')
#                    #Definimos el DataFrame con su separador y encoding
#                    df = pd.read_csv( file_path, encoding=enc['encoding'] )
#    
#                    #Normalizamos
#                    df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
#                    df['precio'] = df['precio'].apply(lambda x: float(x) if(type(x) != str) else np.NaN)
#                    df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
#                    df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
#    
#                    #Lo asignamos con su fecha en el diccionario
#                    for date in datefinder.find_dates(str(file_path)):
#                        df['actualizado'] = date.strftime('%Y-%m-%d')
#                        precios_dicc[date.strftime('%Y-%m-%d')] = df
#    
#        #Definimos si el archivo es JSON
#        elif(str(file_path.suffix) in ['.json']):
#            print('******************************\n DEBUG 6 \n ******************************')
#            #Definimos el DataFrame
#            df = pd.read_json( file_path)
#    
#            #Normalizamos
#            df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
#            df['precio'] = df['precio'].apply(lambda x: float(x) if(type(x) != str) else np.NaN)
#            df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
#            df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
#    
#    
#            #Lo asignamos con su fecha en el diccionario
#            for date in datefinder.find_dates(str(file_path)):
#                df['actualizado'] = date.strftime('%Y-%m-%d')
#                precios_dicc[date.strftime('%Y-%m-%d')] = df
#    
#        #Definimos si el archivo es EXCEL
#        elif(str(file_path.suffix) in ['.xlsx','.xls','xlsm','xlsm']):
#            print('******************************\n DEBUG 7 \n ******************************')
#            xlt = pd.ExcelFile(file_path)
#    
#            #Iteramos sobre las hojas
#            for hoja in xlt.sheet_names:
#            
#                #Definimos el DataFrame
#                df = xlt.parse(hoja)
#    
#                #Normalizamos
#                df['producto_id'] = df['producto_id'].apply(lambda x: int(float(x[-13:])) if(type(x) == str) else x)
#                df['sucursal_id'] = df['sucursal_id'].apply(lambda x: ('{0}-{1}-{2}'.format(int(x.day),int(x.month),int(x.year))) if(str(type(x)) == "<class 'datetime.datetime'>") else x)
#                df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
#                df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
#    
#                #Lo asignamos con su fecha en el diccionario
#                for date in datefinder.find_dates(hoja[-9:]+'_'.replace(' ','_')):
#                    df['actualizado'] = date.strftime('%Y-%m-%d')
#                    precios_dicc[date.strftime('%Y-%m-%d')] = df
#    print('******************************\n DEBUG 8 \n ******************************')
#    #Ordenamos el Diccionario de la Fecha mas antigua hacia la mas reciente
#    precios_dicc = {key:precios_dicc[key][['sucursal_id','producto_id','precio','actualizado']] for key in sorted(precios_dicc)}
#    print('Datasets de la Carpeta Precios Procesados ...\n')
#    print('Fechas ordenadas de los archivos en Precios: ', str(list(precios_dicc.keys())))
#    return precios_dicc
#
#def cargar_precios(): #Funcion para cargar los precios desde la Base de Datos
#    df_precios = pd.read_sql_table('precio',db_engine,index_col=False)
#    df_precios['sucursal_id'] = df_precios['sucursal_id'].apply(lambda x: str(x))
#    df_precios['producto_id'] = df_precios['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
#    df_precios = df_precios[['sucursal_id','producto_id','precio','actualizado']]
#    return df_precios
#
#def unificar_precios(**context): #Funcion para unificar los precios cargados de la Base de Datos con los Precios Cargados de los archivos
#
#    #Transformamos y normalizamos los datos
#    precios_dicc = context['task_instance'].xcom_pull(task_ids='Leer_archivos_de_precios')
#    df_precios = context['task_instance'].xcom_pull(task_ids='Cargar_Precios_desde_DB')
#    df_precios['sucursal_id'] = df_precios['sucursal_id'].apply(lambda x: str(x))
#    df_precios['producto_id'] = df_precios['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
#    df_precios = df_precios[['sucursal_id','producto_id','precio','actualizado']]
#    df_output = df_precios[['sucursal_id','producto_id']]
#
#    #Generamos un DataFrame con la unificacion de Sucursales y Productos encontrados en los archivos y en la Base de Datos
#    for i in range(0,len(list(precios_dicc.keys()))):
#
#        df_output = pd.concat([df_output,precios_dicc[list(precios_dicc.keys())[i]][['sucursal_id','producto_id']]])
#
#    df_output = df_output.drop_duplicates().sort_values(by=['sucursal_id','producto_id']).dropna()
#    #Le ponemos los precios con su respectiva fecha desde la base de datos
#    df_precios = pd.merge(df_output, df_precios,  how='left', left_on=['sucursal_id','producto_id'], right_on = ['sucursal_id','producto_id'])
#    
#    #Comparamos las fechas para asignarle el valor mas reciente al DataFrame Final
#    for i in range(0,len(list(precios_dicc.keys()))):
#        df1 = pd.merge(df_precios, precios_dicc[list(precios_dicc.keys())[i]],  how='left', left_on=['sucursal_id','producto_id'], right_on = ['sucursal_id','producto_id'])
#        df1['precio'] = np.nan
#        df1['actualizado'] = np.nan
#        df1['actualizado'] = df1['actualizado'].apply(lambda x: str(x))
#        for index in range(0,int(len(df1))):
#            fecha_x = df1['actualizado_x'][index]
#            fecha_y = df1['actualizado_y'][index]
#            precio_x = df1['precio_x'][index]
#            precio_y = df1['precio_y'][index]
#            if(pd.isna(precio_y) and not(pd.isna(precio_x))):
#                df1.at[index,'precio'] = precio_x
#                df1.at[index,'actualizado'] = fecha_x
#                continue
#            elif(pd.isna(precio_x) and not(pd.isna(precio_y))):
#                df1.at[index,'precio'] = precio_y
#                df1.at[index,'actualizado'] = fecha_y
#                continue
#            if pd.isna(fecha_x) or pd.isna(fecha_y):
#                continue
#            else:
#                fecha_x_int = float(fecha_x.replace('-',''))
#                fecha_y_int = float(fecha_y.replace('-',''))
#                if fecha_x_int > fecha_y_int:
#                    df1.at[index,'precio'] = precio_x
#                    df1.at[index,'actualizado'] = fecha_x
#                    continue
#                elif fecha_x_int < fecha_y_int:
#                    df1.at[index,'precio'] = precio_y
#                    df1.at[index,'actualizado'] = fecha_y
#                    continue
#                elif fecha_x_int == fecha_y_int:
#                    df1.at[index,'precio'] = precio_y
#                    df1.at[index,'actualizado'] = fecha_y
#                    continue
#                else: continue
#        df_precios = df1[['sucursal_id','producto_id','precio','actualizado']].copy()
#    
#    
#    #Limpiamos otra vez
#    df_precios['producto_id'] = df_precios['producto_id'].apply(lambda x: str(str(x)[-13:]))
#    df_precios = df_precios.drop_duplicates().dropna()
#    return df_precios
#
#def generar_csvs(**context): #Generamos los csvs de los archivos de entrada (Sin contar la Base de Datos)
#    precios_dicc = context['task_instance'].xcom_pull(task_ids='Leer_archivos_de_precios')
#    for key in precios_dicc.keys():
#        precios_dicc[key].to_csv(r'/opt/airflow/data/Carga/Salida/Precios_Semana_{}.csv'.format(key), index_label = False)
#
#def precios_excel(**context): #Generamos un excel con las primeras entradas de la Tabla Final de precios
#    df_precios = context['task_instance'].xcom_pull(task_ids='Unificar_precios_Sobreescribir_fecha_mas_reciente')
#    df_precios.head(1008576).to_excel(r'/opt/airflow/data/Carga/Salida/precios.xlsx')
#
#def insertar_precio(**context):#Insertamos la tabla precio en la base de datos, no sin antes dropear la anterior
#    drop_table(table_name='precio',engine=db_engine)
#    time.sleep(10)
#    df_precios = context['task_instance'].xcom_pull(task_ids='Unificar_precios_Sobreescribir_fecha_mas_reciente')
#    df_precios.to_sql('precio',db_engine)

#DAG de Airflow
with DAG(dag_id='Prueba12523',start_date=datetime.datetime(2022,8,25),schedule_interval='@once') as dag:


    t_listar_precios = PythonOperator(task_id='Listar_Precios',python_callable=listar_archivos)

    #t_leer_precios = PythonOperator(task_id='Leer_archivos_de_precios',python_callable=leer_precios)
    #t_cargar_precios = PythonOperator(task_id='Cargar_Precios_desde_DB',python_callable=cargar_precios)
#
    #t_unificar_precios = PythonOperator(task_id='Unificar_precios_Sobreescribir_fecha_mas_reciente',python_callable=unificar_precios)
#
    #t_generar_csvs = PythonOperator(task_id='Generar_CSVs',python_callable=generar_csvs)
    #t_precios_excel = PythonOperator(task_id='Generar_Excel_de_Precios',python_callable=precios_excel)
    #t_insertar_precio = PythonOperator(task_id='DB_Generar_Tabla_Precio',python_callable=insertar_precio)

    t_listar_precios #>> [t_leer_precios,t_cargar_precios] >> t_unificar_precios >> [t_generar_csvs,t_precios_excel,t_insertar_precio]
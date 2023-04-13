#!/usr/bin/env python

"""BigQuery I/O PySpark example."""

from pyspark.sql import SparkSession
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

import os
import findspark

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "dataproc-test-381100"
spark.conf.set('temporaryGcsBucket', bucket)

# GET request to URL:
url = "https://www.formula1.com/en/results.html/2022/fastest-laps.html"
response = requests.get(url)

# Analizar el contenido HTML usando BeautifulSoup
soup = BeautifulSoup(response.content, "html.parser")

# Encontrar la tabla de resultados para los conductores
table = soup.find("table", attrs={"class": "resultsarchive-table"})

# Extraer los datos de la tabla en un DataFrame de pandas
df = pd.read_html(str(table))[0]

# Seleccionar sólo las columnas necesarias
df = df[["Grand Prix", "Driver", "Car", "Time"]]

# Crear una nueva columna "Acronym" con los últimos 3 caracteres de la columna "Driver"
df["Acronym"] = df["Driver"].str.slice(-3)

# Quitar los últimos 3 caracteres de la columna "Driver"
df["Driver"] = df["Driver"].str.slice(stop=-3)


now = datetime.now()

epoch_time = int(now.timestamp())
  
df["Date"] = epoch_time

new_name = {"Car": "Team"}
df = df.rename(columns=new_name)

# Convertir la columna "PTS" a un tipo de datos de cadena
#df["Laps"] = df["Laps"].astype(str)
df["Date"] = df["Date"].astype(str)

# Descarga del df en archivo .CSV
df.to_csv("resultados_teams.csv", index=False)

os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
os.environ['SPARK_HOME'] = '/content/spark-3.1.2-bin-hadoop3.2'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'

findspark.init()

sc = pyspark.SparkContext("local[*]")
spark = SparkSession(sc)

!wget --continue /resultados_teams.csv
spark_df = spark.read.csv('/resultados_teams.csv')
spark_df.show()

df = spark.read.option("header",True).csv('/resultados_teams.csv')
df.show()

# Load data from BigQuery.
"""words = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data.samples.shakespeare') \
  .load()
"""





"""words.createOrReplaceTempView('words')

# Perform word count.
word_count = spark.sql(
    'SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word')
word_count.show()
word_count.printSchema()

# Saving the data to BigQuery
word_count.write.format('bigquery') \
  .option('table', 'wordcount_dataset.wordcount_output') \
  .save()
"""

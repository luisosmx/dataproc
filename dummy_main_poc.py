from datetime import datetime
from pyspark.sql import SparkSession
import requests
from bs4 import BeautifulSoup
from pyspark.sql import DataFrame

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# GET request to URL:
url = "https://www.formula1.com/en/results.html/2022/drivers.html"
response = requests.get(url)

def data_extraction(response) -> DataFrame:
    from pyspark.sql.types import StructType,StructField, StringType, IntegerType
    data2 = [("James","","Smith","36636","M",3000),
        ("Michael","Rose","","40288","M",4000),
        ("Robert","","Williams","42114","M",4000),
        ("Maria","Anne","Jones","39192","F",4000),
        ("Jen","Mary","Brown","","F",-1)
      ]

    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
      ])
    
    df = spark.createDataFrame(data=data2,schema=schema)
    df.printSchema()
    df.show(truncate=False)
    return df

def data_transformation(df):
    # Seleccionar sólo las columnas necesarias
    df = df[["Pos", "Driver", "Nationality", "Car", "PTS"]]

    # Crear una nueva columna "Acronym" con los últimos 3 caracteres de la columna "Driver"
    df["Acronym"] = df["Driver"].str.slice(-3)

    # Quitar los últimos 3 caracteres de la columna "Driver"
    df["Driver"] = df["Driver"].str.slice(stop=-3)

    now = datetime.now()

    epoch_time = int(now.timestamp())

    df["Date"] = epoch_time

    new_name = {"Pos": "Position","Car": "Team", "PTS": "Points"}
    df = df.rename(columns=new_name)

    # Convertir la columna "PTS" a un tipo de datos de cadena
    df["Points"] = df["Points"].astype(str)
    df["Date"] = df["Date"].astype(str)
    df["Position"] = df["Position"].astype(str)
    return df

def load_data(df):
    try:
        # Configuración de SparkSession
        spark = SparkSession.builder \
                            .appName("F1DataPipeline") \
                            .getOrCreate()

        # Convertir Pandas DataFrame a PySpark DataFrame
        spark_df = spark.createDataFrame(df)

        # Escribir datos en formato parquet
        spark_df.write.mode('overwrite').parquet("gs://dataproc-test-381100/main.py")

        return "OK"
    except Exception as e:
        print("########################")
        print(str(e))
        print("########################")
        return "Error"

#response = requests.get(url)
drivers = data_extraction(None)
#df = data_transformation(df)
#result = load_data(df)



# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "dataproc-test-381100"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
#drivers = spark.read.format('bigquery') \
#   .option('table', 'airflow-gke-381100.data_f1.results_drivers') \
#   .load()
drivers.createOrReplaceTempView('drivers')

# Load all data
data_f1 = spark.sql(
    """SELECT 
            * 
        FROM drivers 
        """)
data_f1.show()
data_f1.printSchema()

# Saving the data to BigQuery
data_f1.write.format('bigquery') \
  .option('table', 'airflow-gke-381100.data_f1.results_dataprocalready') \
  .option('temporaryGcsBucket', 'dataproc-test-381100') \
  .mode('append') \
  .save()

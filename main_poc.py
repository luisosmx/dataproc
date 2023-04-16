from datetime import datetime
from pyspark.sql import SparkSession
import requests
from bs4 import BeautifulSoup
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
import json


spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# GET request to URL:
#url = "https://www.formula1.com/en/results.html/2022/drivers.html"
#response = requests.get(url)

def data_extraction(response) -> DataFrame:
    from bs4 import BeautifulSoup
    from pyspark.sql import SparkSession
    import requests

    url = "https://www.formula1.com/en/results.html/2022/races.html"
    page = requests.get(url)

    soup = BeautifulSoup(page.content, 'html.parser')

    table = soup.find_all('table', class_='resultsarchive-table')[0]
    rows = table.find_all('tr')

    data = []

    for row in rows[1:]:
        cols = row.find_all('td')
        race_name = cols[1].get_text().strip()
        race_date = cols[2].get_text().strip()
        winner = cols[3].get_text().strip()
        car = cols[4].get_text().strip()
        laps = cols[5].get_text().strip()
        time = cols[6].get_text().strip()
        data.append([race_name, race_date, winner, car, laps, time])

    #columns = ['GRAND PRIX', 'DATE', 'WINNER', 'CAR', 'LAPS', 'TIME']

    # Definir el esquema
    schema = StructType([
        StructField("GRAND_PRIX", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("WINNER", StringType(), True),
        StructField("CAR", StringType(), True),
        StructField("LAPS", StringType(), True),
        StructField("TIME", StringType(), True)
    ])
    df = spark.createDataFrame(data=data, schema=schema)
    #imprime esquema
    print("imprime esquema")
    df.printSchema()
    print("imprime datos")
    df.show(truncate=False)
    return df, schema

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
drivers, schema = data_extraction(None)
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
  .option('table', 'airflow-gke-381100.data_f1.test') \
  .option('temporaryGcsBucket', 'dataproc-test-381100') \
  .option('schema', json.dumps(schema.jsonValue())) \
  .mode('append') \
  .save()
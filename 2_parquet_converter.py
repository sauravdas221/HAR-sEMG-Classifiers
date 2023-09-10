import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

import time

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("Data Stager") \
    .config("spark.driver.memory", '8g') \
    .config("spark.jars", "postgresql_jars/postgresql-42.2.18.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Get the start time
st = time.time()

print("Reading the CSV files..")
semg_df = spark.read.csv("dataset_csv_stage/", header=True, inferSchema=True)
semg_df.printSchema()
print("Completed reading the CSV files.")

#Writing to DB:
print("Repartitioning and writing as parquet..")
semg_df = semg_df.repartition(21, 'activity')
semg_df.write.mode('overwrite').parquet("dataset_parquet_stage/")
print("Completed repartitioning and writing.")

print('Writing to database..')
semg_df.write.format('jdbc').option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "semg_all_subjects") \
    .option("user", "postgres").option("password", "postgres").save(mode='append')
print("Completed writing to database.")
print('Processing complete.')

# Get the end time
et = time.time()

# Calculate the execution time
elapsed_time = et - st
print('Execution time:', elapsed_time, 'seconds')
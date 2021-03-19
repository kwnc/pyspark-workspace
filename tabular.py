import os
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark_session = SparkSession.builder.appName("Tabularne dane").master("spark://192.168.222.129:7077").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")




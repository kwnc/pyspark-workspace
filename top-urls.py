from pyspark.sql import SparkSession

import pyspark.sql.functions as F

# Utwórz SparkSession
spark_session = SparkSession.builder.appName("Top URLs").getOrCreate()
spark_session.sparkContext.setLogLevel("ERROR")

# Monitoruj katolog logi dla nowych plików z logami i wczytaj wiersze tych plików
accessLines = spark_session.readStream.text("./logs")

# Wzory poszczególnych informacji wierszy
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(F.regexp_extract('value', hostExp, 1).alias('host'),
                            F.regexp_extract('value', timeExp, 1).alias('timestamp'),
                            F.regexp_extract('value', generalExp, 1).alias('method'),
                            F.regexp_extract('value', generalExp, 2).alias('endpoint'),
                            F.regexp_extract('value', generalExp, 3).alias('protocol'),
                            F.regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                            F.regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logsDF2 = logsDF.withColumn("eventTime", F.current_timestamp())

# Na bieżąco licz punkty kontrolne
endpointCounts = logsDF2 \
    .groupBy(F.window(F.col("eventTime"), "30 seconds", "10 seconds"), F.col("endpoint")).count()

sortedEndpointCounts = endpointCounts.orderBy(F.col("count").desc())

# Wyświetl stream do konsoli
query = sortedEndpointCounts.writeStream.outputMode("complete").format("console") \
    .queryName("counts").start()

# Poczekaj na ukończenie skryptów
query.awaitTermination()

# Zatrzymaj sesję
spark_session.stop()

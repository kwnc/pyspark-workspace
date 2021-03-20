from pyspark.sql import SparkSession, functions as F

spark_session = SparkSession.builder.appName("Popularne słowa") \
    .master("spark://192.168.222.132:7077") \
    .getOrCreate()
spark_session.sparkContext.setLogLevel("FATAL")

book = spark_session.read.text("./data/books/*.txt")

lines = book.select(F.split(book.value, " ").alias("line"))

words = lines.select(F.explode(F.col("line")).alias("word"))

words_lower = words.select(F.lower(F.col("word")).alias("word_lower"))

words_clean = words_lower.select(F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word"))

words_nonull = words_clean.where(F.col("word") != "")

results = words_nonull.groupBy(F.col("word")).count()

results.orderBy("count", ascending=False).show(40)

# results.coalesce(1).write.csv('./results_single_partition.csv')

from pyspark.sql import SparkSession, functions as F


def analize_books(spark_session):
    books = spark_session.read.text("./data/books/*.txt")

    lines = books.select(F.split(books.value, " ").alias("line"))

    words = lines.select(F.explode(F.col("line")).alias("word"))

    words_lower = words.select(F.lower(F.col("word")).alias("word_lower"))

    words_clean = words_lower.select(F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word"))

    words_nonull = words_clean.where(F.col("word") != "")

    results = words_nonull.groupBy(F.col("word")).count().show()

    results.orderBy("count", ascending=False).show(50)

    results.coalesce(1).write.csv('./results_single_partition')


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("Popularne słowa") \
        .master("spark://10.111.111.57:7077") \
        .getOrCreate()

    spark_session.sparkContext.setLogLevel("WARN")

    analize_books(spark_session)
    spark_session.stop()

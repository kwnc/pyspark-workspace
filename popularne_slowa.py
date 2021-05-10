from pyspark.sql import SparkSession, functions as F


def analyse_books(spark_session):
    books = spark_session.read.text("/home/lab/data/books/*.txt")
    rows = books.select(F.split(books.value, " ").alias("row"))
    words = rows.select(F.explode(F.col("row")).alias("word"))
    words_lower = words.select(F.lower(F.col("word")).alias("word_lower"))
    words_clean = words_lower.select(F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word"))
    words_nonull = words_clean.where(F.col("word") != "")
    results = words_nonull.groupBy(F.col("word")).count()
    results.orderBy("count", ascending=False).show(50)
    # results.coalesce(1).write.csv('./results_single_partition')


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Popularne słowa") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    analyse_books(spark)
    spark.stop()

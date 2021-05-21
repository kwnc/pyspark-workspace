from pyspark.sql import SparkSession, functions as F


def read_books(path):
    books_df = spark_session.read.text(path)
    return books_df


def count_words(books):
    rows = books.select(F.split(books.value, " ").alias("row"))
    words = rows.select(F.explode(F.col("row")).alias("token"))
    words_lower = words.select(F.lower(F.col("token")).alias("word_lower"))
    words_clean = words_lower.select(F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word"))
    words_nonull = words_clean.where(F.col("word") != "")
    results = words_nonull.groupBy(F.col("word")).count()
    results.orderBy("count", ascending=False).show(40)
    results.coalesce(1).write.csv('./results_single_partition')


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("Popularne slowa").getOrCreate()

    spark_session.sparkContext.setLogLevel("ERROR")

    books_path = """./data/books/*.txt"""
    books_df = read_books(books_path)
    count_words(books_df)
    spark_session.stop()

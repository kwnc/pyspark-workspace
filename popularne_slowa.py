from pyspark.sql import SparkSession, functions as F


def main(spark_session):
    books = spark_session.read.text("./data/books/*.txt")

    lines = books.select(F.split(books.value, " ").alias("line"))

    words = lines.select(F.explode(F.col("line")).alias("word"))

    words_lower = words.select(F.lower(F.col("word")).alias("word_lower"))

    words_clean = words_lower.select(F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word"))

    words_nonull = words_clean.where(F.col("word") != "")

    results = words_nonull.groupBy(F.col("word")).count()

    results.orderBy("count", ascending=False).show(40)

    results.coalesce(1).write.csv('./results_single_partition.csv')


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("Popularne słowa") \
        .getOrCreate()

    spark_session.sparkContext.setLogLevel("WARN")

    main(spark_session)
    spark_session.stop()

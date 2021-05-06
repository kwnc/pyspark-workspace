from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == '__main__':
    spark = SparkSession.builder.appName("AirBnb mllib").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    filePath = """./data/sf-airbnb/sf-airbnb-clean.parquet/"""
    airbnbDF = spark.read.parquet(filePath)
    airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
                    "number_of_reviews", "price").show(5)

    trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)
    print(f"""There are {trainDF.count()} rows in the training set, and {testDF.count()} in the test set""")

    vecAssembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features")
    vecTrainDF = vecAssembler.transform(trainDF)
    vecTrainDF.select("bedrooms", "features", "price").show(10)

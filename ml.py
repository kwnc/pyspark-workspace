from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline


def read_data(file_path):
    airbnb_df = spark.read.parquet(data_path)
    airbnb_df.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
                    "number_of_reviews", "price").show(5)
    return airbnb_df


if __name__ == '__main__':
    spark = SparkSession.builder.appName("AirBnb mllib").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data_path = """./data/sf-airbnb/sf-airbnb-clean.parquet/"""
    airbnb_df = read_data(data_path)

    train_df, test_df = airbnb_df.randomSplit([.8, .2], seed=42)
    print(f"""Istnieje {train_df.count()} wierszy w zbiorze treningowym i {test_df.count()} w zbiorze testowym""")

    vec_assembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features")
    vec_train_df = vec_assembler.transform(train_df)
    vec_train_df.select("bedrooms", "features", "price").show(10)

    lr = LinearRegression(featuresCol="features", labelCol="price")
    lrModel = lr.fit(vec_train_df)

    m = round(lrModel.coefficients[0], 2)
    b = round(lrModel.intercept, 2)
    print(f"""Wzór dla regresji liniowej to: cena = {m}*sypialnie + {b}""")

    pipeline = Pipeline(stages=[vec_assembler, lr])
    pipeline_model = pipeline.fit(train_df)

    pred_df = pipeline_model.transform(test_df)
    pred_df.show(10)
    pred_df.select("bedrooms", "features", "price", "prediction").show(10)

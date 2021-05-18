from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline


def read_data(file_path):
    cars_df = spark.read.load(file_path, format="csv", sep=",", inferSchema="true", header="true")
    result_df = cars_df.toDF("make", "model", "year", "engine_fuel_type", "engine_hp", "engine_cylinders",
                             "transmission_type", "driven_wheels", "number_of_doors", "market_category", "vehicle_size",
                             "vehicle_style", "highway_mpg", "city_mpg", "popularity", "price")
    result_df.select("make", "model", "year", "engine_hp",
                     "vehicle_style", "price").show(5)
    return result_df


def split_data(data_df):
    train_df, test_df = data_df.randomSplit([.8, .2], seed=42)
    print(f"""Zbior danych wejsciowych podzielono na:\n
    Rekordy trenujace:\t{train_df.count()}\n
    Rekordy testujace:\t{test_df.count()}""")
    return train_df, test_df


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Ceny pojazdow").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data_path = """./data/car-data.csv"""
    cars_df = read_data(data_path)

    train_df, test_df = split_data(cars_df)

    vec_assembler = VectorAssembler(inputCols=["year", "engine_hp", "number_of_doors", "popularity"],
                                    outputCol="features",
                                    handleInvalid="skip")
    vec_train_df = vec_assembler.transform(train_df)
    vec_train_df.select("year", "engine_hp", "features", "price").show(10)

    lr = LinearRegression(featuresCol="features", labelCol="price")
    lr_model = lr.fit(vec_train_df)

    m = round(lr_model.coefficients[0], 2)
    b = round(lr_model.intercept, 2)
    print(f"""Wzor dla regresji liniowej to: cena = {m}*konie mechaniczne + {b}""")

    estimator = Pipeline(stages=[vec_assembler, lr])
    estimator_model = estimator.fit(train_df)

    prediction_df = estimator_model.transform(test_df)
    prediction_df.show(10)
    prediction_df.select("year", "engine_hp", "number_of_doors", "popularity", "features", "price", "prediction").show(
        10)

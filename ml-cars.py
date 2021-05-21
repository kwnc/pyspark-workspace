from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import numpy as np


def read_data(file_path):
    cars_df = spark.read.load(file_path, format="csv", sep=",", inferSchema="true", header="true")
    result_df = cars_df.toDF("make", "model", "year", "engine_fuel_type", "engine_hp", "engine_cylinders",
                             "transmission_type", "driven_wheels", "number_of_doors", "market_category", "vehicle_size",
                             "vehicle_style", "highway_mpg", "city_mpg", "popularity", "price")
    result_df.select("make", "model", "year", "engine_hp", "number_of_doors", "highway_mpg", "city_mpg", "popularity",
                     "price").show(5)
    return result_df


def split_data(data_df):
    train_df, test_df = data_df.randomSplit([.8, .2], seed=42)
    print(f"""Zbior danych wejsciowych podzielono na:\n
    Rekordy trenujace:\t{train_df.count()}\n
    Rekordy testujace:\t{test_df.count()}""")
    return train_df, test_df


def create_model(train_dataset):
    vec_train_df = vec_assembler.transform(train_dataset)
    vec_train_df.select("features", "price").show(10)

    lr = LinearRegression(featuresCol="features", labelCol="price")
    lr_model = lr.fit(vec_train_df)

    m = round(lr_model.coefficients[0], 2)
    b = round(lr_model.intercept, 2)
    print("Coefficients: %s" % str(lr_model.coefficients))
    print("Intercept: %s" % str(lr_model.intercept))
    print(f"""Wzor dla regresji liniowej to: cena = {m}*x + {b}""")

    estimator = Pipeline(stages=[vec_assembler, lr])
    estimator_model = estimator.fit(train_dataset)
    return estimator_model


def evaluate_model(model):
    regression_evaluator = RegressionEvaluator(
        predictionCol="prediction",
        labelCol="price",
        metricName="rmse")
    rmse = regression_evaluator.evaluate(model)
    print(f"RMSE is {rmse:.1f}")

    r2 = regression_evaluator.setMetricName("r2").evaluate(model)
    print(f"R2 is {r2}")


def plot_histogram(real_data, prediction):
    numbers_of_records = 2000
    input_data = [np.log(row['price']) for row in real_data.take(numbers_of_records)]
    predicted = [np.log(row['price']) for row in prediction.take(numbers_of_records)]

    plt.figure()
    plt.hist([predicted, input_data], bins=30, log=False)
    plt.legend(('predykcja', 'realne dane'))
    plt.xlabel('cena')
    plt.savefig('result_histogram.png')


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Ceny pojazdow").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data_path = """./data/car-data.csv"""
    cars_df = read_data(data_path)

    (train_df, test_df) = split_data(cars_df)

    numeric_cols = ["year", "engine_hp", "number_of_doors", "highway_mpg", "city_mpg", "popularity"]
    vec_assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features", handleInvalid="skip")
    estimate_model = create_model(train_df)

    prediction_df = estimate_model.transform(test_df)
    prediction_df.show(10)
    prediction_df.select("features", "price", "prediction").show(10)

    evaluate_model(prediction_df)

    plot_histogram(cars_df, prediction_df)

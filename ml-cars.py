from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
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


def train_model(train_dataset):
    numeric_cols = ["year", "engine_hp", "number_of_doors", "highway_mpg", "city_mpg", "popularity"]
    vec_assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features", handleInvalid="skip")

    vec_train_df = vec_assembler.transform(train_dataset)
    vec_train_df.select("features", "price").show(10)

    lr = LinearRegression(featuresCol="features", labelCol="price")
    lr_model = lr.fit(vec_train_df)

    year = round(lr_model.coefficients[0], 2)
    engine_hp = round(lr_model.coefficients[1], 2)
    number_of_doors = round(lr_model.coefficients[2], 2)
    highway_mpg = round(lr_model.coefficients[3], 2)
    city_mpg = round(lr_model.coefficients[4], 2)
    popularity = round(lr_model.coefficients[5], 2)
    b = round(lr_model.intercept, 2)
    print(
        f"""Wzor nauczonego modelu:
        cena = {year}*rok + {engine_hp}*konie_mechaniczne + {number_of_doors}*drzwi + {highway_mpg}*mpg_autostrada 
        + {city_mpg}*mpg_miasto + {popularity}*popularnosc + {b}""")

    estimator = Pipeline(stages=[vec_assembler, lr])
    trained_model = estimator.fit(train_dataset)
    return trained_model


def make_predictions(trained_model, test_df):
    prediction_df = trained_model.transform(test_df)
    prediction_df.select("features", "price", "prediction").show(10)
    return prediction_df


def evaluate_model(model):
    regression_evaluator = RegressionEvaluator(
        predictionCol="prediction",
        labelCol="price",
        metricName="rmse")
    rmse = regression_evaluator.evaluate(model)
    print(f"RMSE = {rmse:.1f}")

    r2 = regression_evaluator.setMetricName("r2").evaluate(model)
    print(f"R2 = {r2}")


def plot_histogram(real_data, prediction):
    numbers_of_records = 2000
    input_data = [np.log(row['price']) for row in real_data.take(numbers_of_records)]
    predicted = [np.log(row['price']) for row in prediction.take(numbers_of_records)]

    plt.figure()
    plt.hist([predicted, input_data], bins=30, log=False)
    plt.legend(('prognozowane ceny', 'realne ceny'))
    plt.xlabel('cena')
    plt.ylabel('ilość')
    plt.savefig('result_histogram.png')


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Ceny pojazdow") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data_path = """./data/car-data.csv"""
    cars_df = read_data(data_path)

    (train_df, test_df) = split_data(cars_df)
    estimate_model = train_model(train_df)
    predictions_df = make_predictions(estimate_model, test_df)
    evaluate_model(predictions_df)
    plot_histogram(cars_df, predictions_df)
    spark.stop()

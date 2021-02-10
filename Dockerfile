# Wybranie bazowego obrazu
FROM jupyter/pyspark-notebook:latest

COPY rdd.ipynb examples/rdd.ipynb
COPY dataframes.ipynb examples/dataframes.ipynb

# Zbiory danych do analiz
COPY life-expectancy.csv examples/life-expectancy.csv
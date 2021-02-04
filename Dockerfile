# Wybierz bazowy obraz
FROM jupyter/pyspark-notebook:latest

COPY rdd.ipynb examples/rdd.ipynb
COPY fakefriends.csv examples/fakefriends.csv
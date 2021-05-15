from pyspark import SparkConf, SparkContext
import csv


def read_data(path):
    lines_rdd = spark_context.textFile(path)
    return lines_rdd


def to_key_value(rdd_line):
    columns = rdd_line.split(',')
    country = columns[0]
    life_expect = int(float(columns[3]))
    return country, life_expect


def max_reduce(key_value_rdd):
    max_life = key_value_rdd.reduceByKey(lambda x, y: max(x, y))
    flipped = max_life.map(lambda x: (x[1], x[0]))
    max_life_sorted = flipped.sortByKey(False)
    results = max_life_sorted.collect()
    return results


if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("Oczekiwana dl. zycia")
    spark_context = SparkContext.getOrCreate(conf=spark_conf)

    data_path = """/home/lab/data/life-expectancy.csv"""
    lines = read_data(data_path)
    parsed_lines = lines.map(to_key_value)
    results = max_reduce(parsed_lines)

    with open('life_expect_results.csv', 'w') as results_file:
        csv_writer = csv.writer(results_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(["country", "max_life_expect"])

        for result in results:
            print(result[1] + " -" + str(result[0]) + " lat")
            csv_writer.writerow([result[1], str(result[0])])

from pyspark import SparkConf, SparkContext


### Metoda przetwarzająca linie
def structure_line(line):
    fields = line.split(',')
    entity = fields[0]
    life_expect = int(float(fields[3]))
    return entity, life_expect


if __name__ == '__main__':
    conf = SparkConf() \
        .setAppName("Oczekiwana dł. życia")
    sc = SparkContext(conf=conf)

    ### Wczytanie danych
    lines = sc.textFile("/home/lab/data/life-expectancy.csv")

    ### Kraje z nadłuższym wskaźnikiem długości życia
    parsed_lines = lines.map(structure_line)
    life_expectancy = parsed_lines.map(lambda x: (x[0], x[1]))
    max_life = life_expectancy.reduceByKey(lambda x, y: max(x, y))

    ### Sortowanie wyników
    flipped = max_life.map(lambda x: (x[1], x[0]))

    max_life_sorted = flipped.sortByKey()

    results = max_life_sorted.collect()
    for result in results:
        print(result[1] + " - " + str(result[0]) + " lat")

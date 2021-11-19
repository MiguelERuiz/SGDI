import sys
import csv
from pyspark.sql import SparkSession

def main(filename, happinessFile):
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    happinessWords = (
        sc.textFile(happinessFile)
        .map(lambda str:{str.split()[0]: float(str.split()[2])})
        .reduce(lambda d1, d2: {**d1, **d2})
    )
    happinesWordsBroadcast = sc.broadcast(happinessWords)

    result = (
        sc.textFile(filename)
        .map(lambda str: list(csv.reader([str]))[0])
        .filter(lambda list: list[1] != "episode_id")
        .map(lambda list : (list[-2], list[1]))
        .filter(lambda tuple: tuple[1] != "episode_id")
        .map(lambda tuple: (tuple[1], calculateTotal(tuple[0], happinesWordsBroadcast)))
        .reduceByKey(lambda str, str2: str + str2)
        .sortBy(lambda tuple: tuple[1], False)
    )


    print(result.collect())
    
def calculateTotal(lineWords, happinesWordsBroadcast) :
    total = 0
    for word in lineWords:
        total += happinesWordsBroadcast.value.get(word, 0)
    return total

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

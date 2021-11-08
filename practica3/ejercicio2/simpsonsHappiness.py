import sys
from pyspark.sql import SparkSession

def main(filename, happinessFile):
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    happinessWords = (
        sc.textFile(happinessFile)
        .map(lambda str:(str.split()[0], str.split()[2]))
    )

    result = (
        sc.textFile(filename)
        .map(lambda str: (str.split(",")[-2].split(), str.split(",")[1]))
        .flatMap(lambda tuple : [(l, tuple[1]) for l in tuple[0]])
        .join(happinessWords)
        .map(lambda tuple: (tuple[1][0], tuple[1][1]))
        .reduceByKey(lambda str, str2: float(str) + float(str2))
        .sortBy(lambda tuple: tuple[1], False)
    )

    print(result.collect())
    
if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

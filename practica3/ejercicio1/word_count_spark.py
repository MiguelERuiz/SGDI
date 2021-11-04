import sys
from pyspark.sql import SparkSession


def main(filename):
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    lines = sc.textFile(filename)
    words = lines.flatMap(lambda x: x.split())
    pairs = words.map(lambda x: (x,1))
    counts = pairs.reduceByKey(lambda x,y: x+y)
    print(counts.collect())
    

if __name__ == "__main__":
    main(sys.argv[1])


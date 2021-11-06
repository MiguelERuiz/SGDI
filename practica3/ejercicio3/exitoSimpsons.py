from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

def main():
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    readCSVAsDataFrame(spark, "simpsons_locations.csv", "locationsFile")
    readCSVAsDataFrame(spark, "simpsons_characters.csv", "charactersFile")
    readCSVAsDataFrame(spark, "simpsons_episodes.csv", "episodesFile")
    readCSVAsDataFrame(spark, "simpsons_script_lines.csv", "scriptFile")

    locations = generateLocationsDataFrame(spark)
    characters = generateCharactersDataFrame(spark)
    scprit = generateScriptDataFrame(spark)

    locationsPearson = generatePearson(spark, ["imdb_rating", "total"], locations)
    locationsPearson.show()

    charactersPearson = generatePearson(spark, ["imdb_rating", "total"], characters)
    charactersPearson.show()

    wordPearson = generatePearson(spark, ["imdb_rating", "totalWord"], scprit)
    wordPearson.show()

    dialogPearson = generatePearson(spark, ["imdb_rating", "totalDialog"], scprit)
    dialogPearson.show()

def generatePearson(spark, columList, dataframe):
    vector_col = "corr_features"
    assembler = VectorAssembler(inputCols=columList, outputCol=vector_col)
    assembled = assembler.transform(dataframe)
    pearson_corr = Correlation.corr(assembled, vector_col)
    corr_list = pearson_corr.head()[0].toArray().tolist()
    pearson_corr_df = spark.createDataFrame(corr_list)
    return pearson_corr_df

def generateLocationsDataFrame(spark):
    totalLocations = spark.sql(
        """
        select distinct s.episode_id as id, count(distinct s.location_id) as total
        from scriptFile as s
        group by s.episode_id
        """
    )
    totalLocations.createOrReplaceTempView("totalLocations")
    return spark.sql(
        """
        select e.id as id, e.imdb_rating, t.total
        from episodesFile as e inner join totalLocations t
        on e.id = t.id
        """
    )

def generateCharactersDataFrame(spark):
    totalWoman = spark.sql(
        """
        select distinct s.episode_id as id, count(distinct s.character_id) as total
        from scriptFile as s inner join charactersFile c on s.character_id = c.id
        where c.gender = 'f'
        group by s.episode_id
        """
    )
    totalWoman.createOrReplaceTempView("totalWoman")

    return spark.sql(
        """
        select e.id as id, e.imdb_rating, t.total
        from episodesFile as e inner join totalWoman t
        on e.id = t.id
        """
    )

def generateScriptDataFrame(spark):
    totalWords = spark.sql(
        """
        select distinct s.episode_id as id, sum(s.word_count) as totalWord, count(s.episode_id) as totalDialog
        from scriptFile as s
        group by s.episode_id
        """
    )
    totalWords.createOrReplaceTempView("totalWords")
    return spark.sql(
        """
        select e.id as id, e.imdb_rating, t.totalWord, t.totalDialog
        from episodesFile as e inner join totalWords t
        on e.id = t.id
        """
    )

def readCSVAsDataFrame(spark, filename, dataframeName):
    dataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(filename)
    dataFrame.createOrReplaceTempView(dataframeName)

if __name__ == "__main__":
    main()
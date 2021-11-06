from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

def main():
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    readCSVAsDataFrame(spark, "simpsons_locations.csv", "locationsFile")
    readCSVAsDataFrame(spark, "simpsons_characters.csv", "charactersFile")
    readCSVAsDataFrame(spark, "simpsons_episodes.csv", "episodesFile")
    readCSVAsDataFrame(spark, "simpsons_script_lines.csv", "scriptFile")

    locations = generateLocationsDataFrame(spark)
    print("PEARSON ",locations.stat.corr("imdb_rating", "total"))
    # locations.show()

    characters = generateCharactersDataFrame(spark)
    print("PEARSON ",characters.stat.corr("imdb_rating", "total"))
    # characters.show()

    scprit = generateScriptDataFrame(spark)
    # scprit.show()
    print("PEARSON ",scprit.stat.corr("imdb_rating", "totalWord"))
    print("PEARSON ",scprit.stat.corr("imdb_rating", "totalDialog"))


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
    return spark.sql(
        """
        select e.id as id, e.imdb_rating, 
            (select count(distinct s.location_id)
            from scriptFile as s
            where s.episode_id = e.id) as total
        from episodesFile as e
        """
    ).fillna(value=0)

def generateCharactersDataFrame(spark):
    return spark.sql(
        """
        select e.id as id, e.imdb_rating,
            (select count(distinct s.character_id)
            from scriptFile as s inner join charactersFile c on s.character_id = c.id
            where s.episode_id = e.id and c.gender = 'f') as total
        from episodesFile as e
        """
    ).fillna(value=0)

def generateScriptDataFrame(spark):
    return spark.sql(
        """
        select e.id as id, e.imdb_rating, 
            (select sum(s.word_count)
            from scriptFile as s
            where e.id = s.episode_id
            and s.speaking_line = true) as totalWord, 
            (select count(s.episode_id)
            from scriptFile as s
            where e.id = s.episode_id
            and s.speaking_line = true) as totalDialog
        from episodesFile as e
        """
    ).fillna(value=0)

def readCSVAsDataFrame(spark, filename, dataframeName):
    dataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(filename)
    dataFrame.createOrReplaceTempView(dataframeName)

if __name__ == "__main__":
    main()
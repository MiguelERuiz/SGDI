# SGDI, Práctica MapReduce - Apache Spark, Sergio García Sánchez, Miguel Ruiz Nieto
# Declaramos que esta solución es fruto exclusivamente de nuestro tra-
# bajo personal. No hemos sido ayudados por ninguna otra persona ni
# hemos obtenido la solución de fuentes externas, y tampoco hemos
# compartido nuestra solución con otras personas. Declaramos además
# que no hemos realizado de manera deshonesta ninguna otra actividad
# que pueda mejorar nuestros resultados ni perjudicar los resultados de
# los demás.
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    readCSVAsDataFrame(spark, "simpsons_locations.csv", "locationsFile")
    readCSVAsDataFrame(spark, "simpsons_characters.csv", "charactersFile")
    readCSVAsDataFrame(spark, "simpsons_episodes.csv", "episodesFile")
    readCSVAsDataFrame(spark, "simpsons_script_lines.csv", "scriptFile")

    locations = generateLocationsDataFrame(spark)
    print("PEARSON ",locations.stat.corr("imdb_rating", "total"))

    characters = generateCharactersDataFrame(spark)
    print("PEARSON ",characters.stat.corr("imdb_rating", "total"))

    scprit = generateScriptDataFrame(spark)
    print("PEARSON ",scprit.stat.corr("imdb_rating", "totalWord"))
    print("PEARSON ",scprit.stat.corr("imdb_rating", "totalDialog"))

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
# Databricks notebook source
# importing packages
from pyspark.sql.types import *
from pyspark.sql.functions import explode, explode_outer, current_timestamp, lit, abs
from functools import reduce
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
# File location
container_path = "/FileStore/tables"
# create widget for bronze to slver ingest date
dbutils.widgets.text("ingest_date", "2022-11-29", "insert ingest date for data")

# Ingest to bronze

def toBronze() {
    container_path = "/FileStore/tables"
    movieSchema = StructType([
        StructField('movie', 
            ArrayType(
                StructType([
                    StructField('Id', IntegerType(), True),
                    StructField('Title', StringType(), True),
                    StructField('Overview', StringType(), True), 
                    StructField('Tagline', StringType(), True),
                    StructField('Budget', DoubleType(), True),
                    StructField('Revenue', DoubleType(), True), 
                    StructField('ImdbUrl', StringType(), True), 
                    StructField('TmdbUrl', StringType(), True),
                    StructField('PosterUrl', StringType(), True),
                    StructField('BackdropUrl', StringType(), True), 
                    StructField('OriginalLanguage', StringType(), True), 
                    StructField('ReleaseDate', TimestampType(), True),
                    StructField('RunTime', IntegerType(), True),
                    StructField('Price', DoubleType(), True),
                    StructField('CreatedDate', TimestampType(), True),
                    StructField('UpdatedDate', TimestampType(), True),
                    StructField('UpdatedBy', StringType(), True),
                    StructField('CreatedBy', StringType(), True), 
                    StructField('genres', 
                        ArrayType(
                            StructType([
                                StructField('id', IntegerType(), True),
                                StructField('name', StringType(), True)
                            ]),True
                        ), True
                    )
                ]), True
            ), True
        )
    ])
    movies0 = spark.read.option("header", "false").schema(movieSchema).json(container_path + "/movie_0.json", multiLine=True)
    movies1 = spark.read.option("header", "false").schema(movieSchema).json(container_path + "/movie_1.json", multiLine=True)
    movies2 = spark.read.option("header", "false").schema(movieSchema).json(container_path + "/movie_2.json", multiLine=True)
    movies3 = spark.read.option("header", "false").schema(movieSchema).json(container_path + "/movie_3.json", multiLine=True)
    movies4 = spark.read.option("header", "false").schema(movieSchema).json(container_path + "/movie_4.json", multiLine=True)
    movies5 = spark.read.option("header", "false").schema(movieSchema).json(container_path + "/movie_5.json", multiLine=True)
    movies6 = spark.read.option("header", "false").schema(movieSchema).json(container_path + "/movie_6.json", multiLine=True)
    movies7 = spark.read.option("header", "false").schema(movieSchema).json(container_path + "/movie_7.json", multiLine=True)
    df0 = movies0.select(explode(movies0.movie))
    df0 = df0.withColumnRenamed('col', 'Movies')
    df1 = movies1.select(explode(movies1.movie))
    df1 = df1.withColumnRenamed('col', 'Movies')
    df2 = movies2.select(explode(movies2.movie))
    df2 = df2.withColumnRenamed('col', 'Movies')
    df3 = movies3.select(explode(movies3.movie))
    df3 = df3.withColumnRenamed('col', 'Movies')
    df4 = movies4.select(explode(movies4.movie))
    df4 = df4.withColumnRenamed('col', 'Movies')
    df5= movies5.select(explode(movies5.movie))
    df5 = df5.withColumnRenamed('col', 'Movies')
    df6 = movies6.select(explode(movies6.movie))
    df6 = df6.withColumnRenamed('col', 'Movies')
    df7 = movies7.select(explode(movies7.movie))
    df7 = df7.withColumnRenamed('col', 'Movies')
    dfs = [df0, df1, df2, df3, df4, df5, df6, df7]
    df = reduce(DataFrame.unionAll, dfs)
    columnNames = ["Movies.id", "Movies.Title", "Movies.Overview", "Movies.Tagline", "Movies.Budget",
                "Movies.Revenue", "Movies.ImdbUrl", "Movies.TmdbUrl", "Movies.BackdropUrl", "Movies.OriginalLanguage",
                "Movies.ReleaseDate", "Movies.RunTime", "Movies.Price", "Movies.CreatedDate", "Movies.UpdatedDate",
                "Movies.UpdatedBy", "Movies.CreatedBy", "Movies.genres"]
    df = df.select(columnNames)
    df = df.withColumn("ingesttime", current_timestamp())
    df = df.withColumn("ingestdate", current_timestamp().cast("date"))
    df = df.withColumn("status", lit("new"))
    df = df.withColumn("source", lit("sep adb assignment"))
    df = df.distinct()
    df.write.mode("overwrite").option("path", "/mnt/movies/bronze").saveAsTable("bronze")
}

def toSilver(ingest_date = '2022-11-29') {
    # quarantining data with negative runtime and saving to silver
    ingest_date = dbutils.widgets.get("ingest_date")
    df = spark.read.load("/mnt/movies/bronze").filter("status = 'new'").filter("ingestdate = '2022-11-29'")
    quarantined = df.filter("RunTime < 0")
    df = df.filter("RunTime >= 0")
    df.write.format("delta").mode("append").option("path", "/mnt/movies/silver").saveAsTable("silver")
    # update bronze table
    bronze = DeltaTable.forPath(spark, "/mnt/movies")
    silver = spark.read.load("/mnt/movies/silver")
    silverAugmented = silver.withColumn("status", lit('loaded'))
    update = {'bronze.status' : 'clean.status'}
    update_match = "bronze.id = clean.id"
    (
        bronze.alias('bronze')
        .merge(silverAugmented.alias('clean'), update_match)
        .whenMatchedUpdate(set = update)
        .execute()
    )
    # update quarantined records 
    silverAugmented = quarantined.withColumn('status', lit("quarantined"))
    update = {'status' : 'quarantine.status'}
    update_match = "bronze.id = quarantine.id"
    (
        bronze.alias('bronze')
        .merge(silverAugmented.alias('quarantine'), update_match)
        .whenMatchedUpdate(set = update)
        .execute()
    )
}


def silverUpdate() {
    # loading quarantined data
    quarantine_df = spark.read.load("/mnt/movies/bronze").filter("status = 'quarantined'")
    quarantine_cleaned = quarantine_df.withColumn("RunTime", abs(quarantine_df.RunTime))
    quarantine_cleaned_Augmented = quarantine_cleaned.withColumn("status", lit('loaded'))
    #update silver table
    quarantine_cleaned_Augmented.write.format("delta").mode("append").option("path", "/mnt/movies/silver").saveAsTable("silver")
    #update bronze table
    bronze = DeltaTable.forPath(spark, "/mnt/movies/bronze")
    update = {'status' : 'quarantine_cleaned.status', 'RunTime' : 'quarantine_cleaned.RunTime'}
    update_match = "bronze.id = quarantine_cleaned.id"
    (
        bronze.alias('bronze')
        .merge(quarantine_cleaned_Augmented.alias('quarantine_cleaned'), update_match)
        .whenMatchedUpdate(set = update)
        .execute()
    )
}
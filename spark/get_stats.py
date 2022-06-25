from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, sum
from pyspark.sql.types import StringType

from helpers import Helpers

def get_first_goal_stats(df):
    goals_df = df\
        .alias('goals')\
        .where('comment == "Gol"')\
        .orderBy(col('minute').asc())\
        .collect()[0]

    first_goal_minute = goals_df.minute
    
    check_minute_first_goal = col('minute') <= first_goal_minute

    df = df\
        .alias('comments')\
        .withColumn('Chutes', when((col('comment').contains('Chute')) & (check_minute_first_goal), 1).otherwise(0))\
        .withColumn('Chutes para fora', when((col('comment').contains('Chute para fora')) & (check_minute_first_goal), 1).otherwise(0))\
        .withColumn('Chutes bloqueados', when((col('comment').contains('Chute bloqueado')) & (check_minute_first_goal), 1).otherwise(0))\
        .withColumn('Chutes na trave', when((col('comment').contains('Chute na trave')) & (check_minute_first_goal), 1).otherwise(0))\
        .withColumn('Chutes no gol', when((col('comment').contains('Chute no gol')) & (check_minute_first_goal), 1).otherwise(0))\
        .withColumn('Cantos', when((col('comment').contains('Canto')) & (check_minute_first_goal), 1).otherwise(0))\
        .where('comment != ""')\
        .select('*')\
        .orderBy(col('minute').desc())\
        .groupBy('comments.match_id', 'comments.team_name')\
        .agg(
            sum('Chutes').alias('CG'),
            sum('Chutes para fora').alias('CFG'),
            sum('Chutes bloqueados').alias('CBG'),
            sum('Chutes na trave').alias('CTG'),
            sum('Chutes no gol').alias('CGG'),
            sum('Cantos').alias('CaG'),
        )

    return df

def get_ht_stats(df):
    check_is_ht = (col('minute') < 45) | (col('represent_min').contains('45'))
    
    df = df\
        .alias('comments')\
        .withColumn('Chutes', when((col('comment').contains('Chute')) & (check_is_ht), 1).otherwise(0))\
        .withColumn('Chutes para fora', when((col('comment').contains('Chute para fora')) & (check_is_ht), 1).otherwise(0))\
        .withColumn('Chutes bloqueados', when((col('comment').contains('Chute bloqueado')) & (check_is_ht), 1).otherwise(0))\
        .withColumn('Chutes na trave', when((col('comment').contains('Chute na trave')) & (check_is_ht), 1).otherwise(0))\
        .withColumn('Chutes no gol', when((col('comment').contains('Chute no gol')) & (check_is_ht), 1).otherwise(0))\
        .withColumn('Cantos', when((col('comment').contains('Canto')) & (check_is_ht), 1).otherwise(0))\
        .where('comment != ""')\
        .select('*')\
        .orderBy(col('minute').desc())\
        .groupBy('comments.match_id', 'comments.team_name')\
        .agg(
            sum('Chutes').alias('CHT'),
            sum('Chutes para fora').alias('CFHT'),
            sum('Chutes bloqueados').alias('CBHT'),
            sum('Chutes na trave').alias('CTHT'),
            sum('Chutes no gol').alias('CGHT'),
            sum('Cantos').alias('CaHT'),
        )

    return df

def get_ft_stats(df):
    df = df\
        .alias('comments')\
        .withColumn('Chutes', when(col('comment').contains('Chute'), 1).otherwise(0))\
        .withColumn('Chutes para fora', when(col('comment').contains('Chute para fora'), 1).otherwise(0))\
        .withColumn('Chutes bloqueados', when(col('comment').contains('Chute bloqueado'), 1).otherwise(0))\
        .withColumn('Chutes na trave', when(col('comment').contains('Chute na trave'), 1).otherwise(0))\
        .withColumn('Chutes no gol', when(col('comment').contains('Chute no gol'), 1).otherwise(0))\
        .withColumn('Cantos', when(col('comment').contains('Canto'), 1).otherwise(0))\
        .where('comment != ""')\
        .select('*')\
        .orderBy(col('minute').desc())\
        .groupBy('comments.match_id', 'comments.team_name')\
        .agg(
            sum('Chutes').alias('CFT'),
            sum('Chutes para fora').alias('CFFT'),
            sum('Chutes bloqueados').alias('CBFT'),
            sum('Chutes na trave').alias('CTFT'),
            sum('Chutes no gol').alias('CGFT'),
            sum('Cantos').alias('CaFT'),
        )

    return df

def get_stats(spark):
    treat_stats = udf(Helpers().treat_stats, StringType())

    stats = spark.read.json(
        "/home/lucas/pipeline-data/datalake/silver/who_scored"
    )

    stats = stats\
        .alias('comments')\
        .withColumn("comment", treat_stats(col('text')))\
        .select('*')

    goals = get_first_goal_stats(stats)
    ht = get_ht_stats(stats)
    ft= get_ft_stats(stats)

    stats_df = goals.alias('goals')\
        .join(ht, ['match_id', 'team_name'])\
        .join(ft, ['match_id', 'team_name'])\
        .select('goals.match_id', 'goals.team_name',
        'goals.CG', 'goals.CFG','goals.CBG','goals.CTG','goals.CGG','goals.CaG',
        'CHT', 'CFHT','CBHT','CTHT','CGHT','CaHT',
        'CFT', 'CFFT','CBFT','CTFT','CGFT','CaFT',
        )

    stats_df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .json(
        "/home/lucas/pipeline-data/datalake/gold/stats"
    )

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("football_get_stats")\
        .getOrCreate()
    
    get_stats(spark)
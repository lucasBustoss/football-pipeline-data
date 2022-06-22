from os.path import join
import argparse

from pyspark import SparkContext
from pyspark.sql.functions import col, udf, explode
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from helpers import Helpers

def export_json(df, dest):
    df.coalesce(1).write.mode('overwrite').json(dest)

def get_odds_portal(df):
    op_df = df\
        .select(
            'home_team',
            'away_team',
            'odd_home',
            'odd_draw', 
            'odd_away'
        )
    
    treat_names = udf(Helpers().treat_names, StringType())
    op_df = op_df.withColumn("home_team", treat_names(col('home_team')))
    op_df = op_df.withColumn("away_team", treat_names(col('away_team')))

    return op_df

def get_who_scored(df):
    ws_comments_df = df\
        .select(explode('matches').alias('matches'))\
        .select(
            'matches.match_id', 
            explode('matches.comments').alias('comments')
        )\
        .select(
            'match_id',
            'comments.*'
            )

    ws_teams_df = df\
        .select(explode('teams').alias('teams'))\
        .select(
            'teams.id',
            'teams.name'
            )

    ws_df = ws_comments_df.join(
        ws_teams_df, 
        ws_comments_df.team == ws_teams_df.id, 
        'left')

    ws_df = ws_df\
        .selectExpr(
            'match_id',
            'minute',
            'represent_min', 
            'second',
            'name as team_name', 
            'text'
            )

    return ws_df

def football_transform(spark, src, dest, process_date):
    op_raw_df = spark.read.json(src + '/odds_portal')
    ws_raw_df = spark.read.json(src + '/who_scored')
    api_raw_df = spark.read.json(src + '/api_football')
    
    odds_portal_df = get_odds_portal(op_raw_df)
    who_scored_df = get_who_scored(ws_raw_df)

    table_dest = join(dest, '{table_name}', f'process_date={process_date}')

    export_json(odds_portal_df, table_dest.format(table_name="odds_portal"))
    export_json(who_scored_df, table_dest.format(table_name="who_scored"))

if __name__ == '__main__':
    sc = SparkContext()
    sc.addPyFile('/home/lucas/pipeline-data/helpers/helpers.py')
    
    parser = argparse.ArgumentParser(
        description='Spark Football Transformation'
    )
    parser.add_argument('--src', required=True)
    parser.add_argument('--dest', required=True)
    parser.add_argument('--process-date', required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName('twitter_transformation')\
        .getOrCreate()
    
    football_transform(spark, args.src, args.dest, args.process_date)

    """
    spark = SparkSession\
        .builder\
        .appName('twitter_transformation')\
        .getOrCreate()
    football_transform(spark, "/home/lucas/pipeline-data/datalake/bronze/extract_date=2022-04-11", "", "")
    """
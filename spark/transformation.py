from os.path import join
import argparse

from pyspark import SparkContext
from pyspark.sql.functions import col, udf, explode
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType

from helpers import Helpers

def export_json(df, dest):
    df.coalesce(1).write.mode('overwrite').json(dest)

def treat_names(df, shouldDifferTeams):
    treat_names = udf(Helpers().treat_names, StringType())
    
    if (shouldDifferTeams):
        df = df.withColumn("home_team", treat_names(col('home_team')))
        df = df.withColumn("away_team", treat_names(col('away_team')))
    else:
        df = df.withColumn("team_name", treat_names(col('team_name')))

    return df

def get_odds_portal(df):
    op_df = df\
        .select(
            'home_team',
            'away_team',
            'odd_home',
            'odd_draw', 
            'odd_away'
        )
    
    op_df = treat_names(op_df, shouldDifferTeams=True)

    return op_df

def get_who_scored(df):
    ws_comments_df = df\
        .select(explode('matches').alias('matches'))\
        .select(
            col('matches.match_id').alias('match_id'),
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
        .withColumn('match_id', ws_df.match_id.cast(IntegerType()))\
        .withColumn('minute', ws_df.minute.cast(IntegerType()))\
        .withColumn('second', ws_df.second.cast(IntegerType()))\
        .selectExpr(
            'match_id',
            'minute',
            'represent_min', 
            'second',
            'name as team_name', 
            'text'
            )

    ws_df = treat_names(ws_df, shouldDifferTeams=False)

    return ws_df

def get_api_football(df):
    api_df = df\
        .select(explode('response').alias('fixtures'))\
        .selectExpr(
            'fixtures.fixture.id', 'fixtures.fixture.date', 
            'fixtures.league.id as league_id',
            'fixtures.teams.home.name as home_team', 'fixtures.teams.away.name as away_team',
            'fixtures.score.halftime.home as score_home_ht', 'fixtures.score.halftime.away as score_away_ht',
            'fixtures.score.fulltime.home as score_home_ft', 'fixtures.score.fulltime.away as score_away_ft'
            )

    api_df = treat_names(api_df, shouldDifferTeams=True)
    
    return api_df

def football_transform(spark, src, dest, process_date):
    op_raw_df = spark.read.json(src + '/odds_portal')
    #ws_raw_df = spark.read.json(src + '/who_scored')
    api_raw_df = spark.read.json(src + '/api_football')
    
    odds_portal_df = get_odds_portal(op_raw_df)
    #who_scored_df = get_who_scored(ws_raw_df)
    api_football_df = get_api_football(api_raw_df)

    table_dest = join(dest, '{table_name}', f'process_date={process_date}')

    export_json(odds_portal_df, table_dest.format(table_name="odds_portal"))
    #export_json(who_scored_df, table_dest.format(table_name="who_scored"))
    export_json(api_football_df, table_dest.format(table_name="api_football"))

if __name__ == '__main__':
    
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
    
    football_transform(
        spark, 
        'datalake/bronze/extract_date=2022-04-10', 
        'datalake/silver/extract_date=2022-04-10', '')
    """ 
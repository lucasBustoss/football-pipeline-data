from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("football_get_matchs")\
        .getOrCreate()
    
    api = spark.read.json(
        "datalake/silver/api_football"
    )
    
    odds = spark.read.json(
        "datalake/silver/odds_portal"
    )

    football = api.alias('api')\
        .join(
            odds.alias('odds'),
            [
                api.home_team == odds.home_team,
                api.away_team == odds.away_team
            ],
            'inner'
        )\
        .select('api.date', 'api.home_team', 'api.away_team', 
        'odd_home', 'odd_draw', 'odd_away',
        'score_home_ft', 'score_away_ft', 'score_home_ht', 'score_away_ht')

    football.coalesce(1)\
        .write\
        .mode("overwrite")\
        .json(
        "datalake/gold/matchs"
    )
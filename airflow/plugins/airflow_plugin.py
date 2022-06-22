from airflow.plugins_manager import AirflowPlugin
from operators.api_football_operator import ApiFootballOperator
from operators.odds_portal_operator import OddsPortalOperator
from operators.who_scored_operator import WhoScoredOperator

class FootballAirflowPlugin(AirflowPlugin):
    name = "football"
    operators = [ApiFootballOperator, OddsPortalOperator, WhoScoredOperator]
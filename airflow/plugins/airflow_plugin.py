from airflow.plugins_manager import AirflowPlugin
from operators.api_football_operator import ApiFootballOperator
from operators.odds_portal_operator import OddsPortalOperator

class FootballAirflowPlugin(AirflowPlugin):
    name = "football"
    operators = [ApiFootballOperator, OddsPortalOperator]
from airflow.hooks.http_hook import HttpHook
import requests
import json

class ApiFootballHook(HttpHook):
    
    def __init__(self, execution_date, conn_id):
        self.execution_date = execution_date
        self.conn_id = conn_id or 'api_football_default'
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        season = f'season=2022'
        start_time = (
            f'&from={self.execution_date}'
            if self.execution_date
            else "&from=2022-04-01"
        )
        end_time = (
            f'&to={self.execution_date}'
            if self.execution_date
            else "&to=2022-06-15"
        )
        league = f'&league={71}'
        timezone = f'&timezone=America%2FSao_Paulo'

        url = "{}/fixtures?{}{}{}{}{}".format(self.base_url, season, start_time, end_time, league, timezone)

        return url

    def connect_to_endpoint(self, session, url):
        response = requests.Request('GET', url)
        prep = session.prepare_request(response)
        self.log.info(f'URL: {url}')
        return self.run_and_check(session, prep, {}).json()

    def run(self):
        session = self.get_conn()

        url = self.create_url()

        return self.connect_to_endpoint(session, url)

if __name__ == "__main__":
    response = ApiFootballHook('2022').run()
    print(json.dumps(response, indent=4, sort_keys=True))
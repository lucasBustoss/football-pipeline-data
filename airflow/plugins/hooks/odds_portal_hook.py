import random
import time
import datetime

import sys
sys.path.append('/home/lucas/pipeline-data/helpers')

from helpers import Helpers

import re
from airflow.hooks.base_hook import BaseHook
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class OddsPortalHook(BaseHook):
    
    def __init__(self, execution_date):
        self.execution_date = datetime.datetime.fromisoformat(execution_date)

    def wait(self, seconds=None):
        if not seconds:
            seconds = random.randrange(2,8)
        time.sleep(seconds)

    def connect_to_website(self):
        url = 'https://www.oddsportal.com/soccer/brazil/serie-a/results/'

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        self.driver.get(url)

    def get_odds(self, games_rows):
        game_odds = []
        
        for game in games_rows:
            contains_strange_span = False

            stats = game.find_elements(By.XPATH, 'td//a')
            teams = stats[0].get_attribute('innerHTML')

            if (re.search('live-odds-ico-prev', teams)):
                contains_strange_span = True

            if(contains_strange_span):
                teams = stats[1].get_attribute('innerHTML')
                odd_home = stats[2].get_attribute('innerHTML')
                odd_draw = stats[3].get_attribute('innerHTML')
                odd_away = stats[4].get_attribute('innerHTML')
            else:
                odd_home = stats[1].get_attribute('innerHTML')
                odd_draw = stats[2].get_attribute('innerHTML')
                odd_away = stats[3].get_attribute('innerHTML')

            teams_splitted = re.split(' - ', teams)
            home_team = teams_splitted[0].replace('<span class="bold">', '').replace('</span>', '')
            away_team = teams_splitted[1].replace('<span class="bold">', '').replace('</span>', '')

            game_odd = {}
            game_odd['home_team'] = home_team
            game_odd['away_team'] = away_team
            game_odd['odd_home'] = odd_home
            game_odd['odd_draw'] = odd_draw
            game_odd['odd_away'] = odd_away

            game_odds.append(game_odd)

        return game_odds

    def get_games_by_date(self):
        games_tr = []

        pagination = self.driver.find_elements(By.XPATH, '//div[@id="pagination"]//a')
        total_pages = int(pagination[len(pagination)-3].get_attribute('innerHTML').replace('<span>', '').replace('</span>', ''))
        date_found = False
        current_page = 1

        while (not(date_found)):
            self.driver.get(f'https://www.oddsportal.com/soccer/brazil/serie-a/results/#/page/{current_page}/')
            self.wait(3)
            
            games_row = self.driver.find_elements(By.XPATH, '//*[(@id = "tournamentTable")]//table//tbody//tr')
            date = ''

            for game in games_row:
                if(date_found & ('deactivate' in game.get_attribute('class'))):
                    games_tr.append(game)
                
                if(game.get_attribute('class') == 'center nob-border'):
                    if(date_found):
                        break
                    
                    date_splitted = re.split(
                        ' ', 
                        game.find_element(By.XPATH, 'th//span').get_attribute('innerHTML'))
                    
                    day = date_splitted[1] if (date_splitted[0] == 'Yesterday,') | (date_splitted[0] == 'Today,')  else date_splitted[0]
                    month = date_splitted[2] if (date_splitted[0] == 'Yesterday,') | (date_splitted[0] == 'Today,')  else date_splitted[1]

                    date = datetime.datetime(
                        2022, 
                        int(Helpers().treat_month_names(month)), 
                        int(day))

                    if(date == self.execution_date):
                        date_found = True
                        continue
                    else:
                        date_found = False
            
            if(not(date_found)):
                current_page += 1
            
            if(current_page > total_pages):
                break
        
        return games_tr

    def run(self):
        self.connect_to_website()

        self.wait(3)

        games_rows = self.get_games_by_date()

        return self.get_odds(games_rows)

if __name__ == "__main__":
    response = OddsPortalHook('2022-04-08').run()
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

class WhoScoredHook(BaseHook):
    
    def __init__(self, execution_date):
        self.execution_date = datetime.datetime.fromisoformat(execution_date)
        self.game_links = []
        self.list_teams = []
        self.list_matchs = []
        self.now = datetime.datetime.now()

    def wait(self, seconds=None):
        if not seconds:
            seconds = random.randrange(2,8)
        time.sleep(seconds)

    def connect_to_website(self):
        url = 'https://1xbet.whoscored.com/Regions/31/Tournaments/95/Seasons/8984/Stages/20566/Fixtures/Brazil-Brasileir%C3%A3o-2022'

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        self.driver.get(url)

    def get_game_links(self):
        table_games = self.driver.find_element(By.XPATH, '//div[(@class = "divtable-body")]')
        games_divs = table_games.find_elements(By.XPATH, 'div')

        div_date = ''
        date = ''

        for game in games_divs:
            if(game.get_attribute('data-id') == None):
                date_array = game.find_element(By.XPATH, 'div').get_attribute('innerHTML').split(' ')
                year = int(date_array[3])
                month = int(Helpers().treat_month_names(date_array[1]))
                day = int(date_array[2].zfill(2))

                date = datetime.datetime(year, month, day)
                if(date > self.execution_date):
                    break
            else:
                if(date < self.execution_date):
                    continue

                status_span = game.find_element(By.XPATH, 'div[contains(@class, "status")]//span')
                status = status_span.get_attribute('innerHTML')

                if(status != 'FT'):
                    continue;

                match_report = game.find_element(By.XPATH, 'div[contains(@class, "result")]//a')
                self.game_links.append(match_report.get_attribute('href'))

    def paginate(self):
        datepicker_previous = self.driver.find_element(By.XPATH, '//div[(@id="date-controller")]//a')
        datepicker_next = self.driver.find_elements(By.XPATH, '//div[(@id="date-controller")]//a')[-1]
        exists_previous_month = datepicker_previous.get_attribute('title') == 'View previous month'
        current_month = datetime.datetime.now()

        while(exists_previous_month & (self.execution_date < current_month)):
            datepicker_previous.click()
            exists_previous_month = datepicker_previous.get_attribute('title') == 'View previous month'
            self.wait(1)
            
        current_month_div = self.driver.find_element(
                                By.XPATH, '//div[(@id="date-controller")]//a[contains(@class, "date")]//span')\
                                .get_attribute('innerHTML').split(' ')

        month = Helpers().treat_month_names(current_month_div[0])
        current_month = datetime.datetime(int(current_month_div[1]), int(month), 1)
            
        while(current_month <= self.now):
            self.get_game_links()
            datepicker_next.click()
            
            current_month_div = self.driver.find_element(
                                By.XPATH, '//div[(@id="date-controller")]//a[contains(@class, "date")]//span')\
                                .get_attribute('innerHTML').split(' ')

            month = Helpers().treat_month_names(current_month_div[0])
            current_month = datetime.datetime(int(current_month_div[1]), int(month), 1)
            self.wait(1)

    def get_teams(self):
        teams = self.driver.find_elements(By.XPATH, '//a[(@class = "team-name")]')
        
        for team in teams:
            team_name = team.get_attribute('innerHTML')
            team_id = team.get_attribute('href').split('/')[4]
        
            if((team_id, team_name) in self.list_teams):
                continue;
            
            team_dict = {}
            team_dict['id'] = team_id
            team_dict['name'] = team_name

            self.list_teams.append(team_dict)

    def get_comments(self, ul_commentary, match_id):
        list_comments = []
        
        for commentary in ul_commentary:
            comment = {}
            
            comment['minute'] = commentary.find_element(By.XPATH, 'span[(@class = "incident-icon")]').get_attribute('data-minute')
            comment['second'] = commentary.find_element(By.XPATH, 'span[(@class = "incident-icon")]').get_attribute('data-second')
            comment['represent_min'] = commentary\
                .find_element(By.XPATH, 'span[(@class = "commentary-minute")]').get_attribute('innerHTML')
            comment['text'] = commentary.find_element(By.XPATH, 'span[(@class = "commentary-text")]').get_attribute('innerHTML')
            comment['team'] = commentary.find_element(By.XPATH, 'span[(@class = "incident-icon")]').get_attribute('data-team-id')
            

            if(
                (
                    comment['minute'], 
                    comment['second'], 
                    comment['represent_min'], 
                    comment['text'], 
                    comment['team']
                ) in list_comments):
                    continue;
            
            list_comments\
                .append(comment)

        return list_comments

    def get_comments_pagination(self, match_id):
        match_centre_current_page = self.driver.find_element(By.XPATH, '//div[(@class="page-info")]//span[(@class="current-page")]').get_attribute('innerHTML')
        match_centre_total_page = self.driver.find_element(By.XPATH, '//div[(@class="page-info")]//span[(@class="total-pages")]').get_attribute('innerHTML')
        match = {}
        match['match_id'] = match_id
        list_comments = []

        for number in range(int(match_centre_current_page), int(match_centre_total_page) + 1):
            ul_commentary_items = self.driver.find_elements(
                By.XPATH, 
                '//div[(@id="match-commentary")]//ul[(@class = "commentary-items")]//li[(@class = "commentary-item")]')

            list_comments += self.get_comments(ul_commentary_items, match_id)
            
            match_centre_navigation_next = self.driver.find_element(By.XPATH, '//div[(@class="page-navigation")]//span[(@data-page="next-page")]')
            match_centre_navigation_next.click()
            
            self.wait(1)
        
        match['comments'] = list_comments
        self.list_matchs.append(match)

    def get_match_report(self, match_link):
        self.driver.get(match_link)
        self.wait(3)
        
        self.get_teams()
        
        match_commentary_click = self.driver.find_element(By.XPATH, '//a[(@href = "#match-commentary")]').click()
        self.wait(1)
        
        
        start_link = 'Matches/'
        end_link = '/Live'

        match_id = re.search('%s(.*)%s' % (start_link, end_link), match_link).group(1)
        
        self.get_comments_pagination(match_id)

    def run(self):
        self.connect_to_website()

        self.wait(3)

        self.paginate()

        for link in self.game_links:
            self.get_match_report(link)
            
        json = {}
        json['matches'] = self.list_matchs
        json['teams'] = self.list_teams

        return json

if __name__ == "__main__":
    response = WhoScoredHook('2022').run()
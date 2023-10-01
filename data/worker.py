import json
import os
import requests
import time
from datetime import datetime, timedelta
from loguru import logger

ID_ROLES_LIST = ["156", "160", "10", "12", "150", "25", "165", "34", "36", "73", "155", "96", "164",
                 "104", "157", "107", "112", "113", "148", "114", "116", "121", "124", "125", "126"]
DEFAULT_MAX_REC_RETURNED = 2000
URL = 'https://api.hh.ru/vacancies'
DEFAULT_MAX_STEP_SIZE = 60 * 60*10
class Warker:
    def __init__(self, date_last, date_to):
        self.date_last = date_last
        self.date_to = date_to

    def api_req(self, page, date_from, date_to):
        params = {
            'per_page': 100,
            'page': page,
            'date_from': f'{date_from.isoformat()}',
            'date_to': f'{date_to.isoformat()}'}
        req = requests.get(URL, params)
        data = req.content.decode()
        data = json.loads(data)
        req.close()
        return data

    def get_time_step(self, date_left, date_right):
        if date_left == self.date_to - DEFAULT_MAX_STEP_SIZE and date_right == self.date_to:
            data = self.api_req(0, self.comvert_seconds_in_time(date_left), self.comvert_seconds_in_time(date_right))
            if data['found'] < DEFAULT_MAX_REC_RETURNED:
                print(data['found'])
                return date_left, data['pages']

        mid = (date_right + date_left) / 2
        data = self.api_req(0, self.comvert_seconds_in_time(mid), self.comvert_seconds_in_time(self.date_to))
        if data['found'] > DEFAULT_MAX_REC_RETURNED:
            logger.info('больше')
            return self.get_time_step(mid, date_right)
        elif data['found'] < 1900:
            logger.info('меньше')
            return self.get_time_step(date_left, mid)
        logger.info(f'равно = {mid}')
        return mid, data['pages']


    def comvert_time_in_seconds(self, data):
        return (data - self.date_last).days * 24 * 60 * 60

    def comvert_seconds_in_time(self, data):
        return self.date_last + timedelta(days=data/(24 * 60 * 60))
    def run(self):
        logger.info(f'Запуск парсера. Временной интервал: От {self.date_to} --> до --> {self.date_last}')
        self.date_to = self.comvert_time_in_seconds(self.date_to)
        while self.date_to != 0:
            while self.date_to != self.date_to - DEFAULT_MAX_STEP_SIZE:
                step, pages = self.get_time_step(self.date_to - DEFAULT_MAX_STEP_SIZE, self.date_to)
                self.date_to -= step
        logger.info(f'Парсинг id вакансий закончен!')
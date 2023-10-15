import json
import requests
import time
from datetime import datetime, timedelta
from loguru import logger
import queue
import psycopg2
from psycopg2 import IntegrityError
from fake_useragent import UserAgent
import random


#
ID_ROLES_LIST = ["156", "160", "10", "12", "150", "25", "165", "34", "36", "73", "155", "96", "164",
                 "104", "157", "107", "112", "113", "148", "114", "116", "121", "124", "125", "126"]
DEFAULT_MAX_REC_RETURNED = 2000
URL = 'https://api.hh.ru/vacancies'
DEFAULT_MAX_STEP_SIZE = 60 * 30
DEFAULT_MIN_STEP_SIZE = 300
PROXIES_LIST = [None, {'http': '149.126.221.237:50100', 'https': '149.126.221.237:50100'},
                {'http': '149.126.221.253:50100', 'https': '149.126.221.253:50100'},
                {'http': '149.126.220.77:50100', 'https': '149.126.220.77:50100'},
                {'http': '149.126.223.63:50100', 'https': '149.126.223.63:50100'},
                {'http': '94.137.78.93:50100', 'https': '94.137.78.93:50100'},
                {'http': '46.3.133.17:50100', 'https': '46.3.133.17:50100'}]


class Worker:

    queue_a = queue.Queue()
    queue_b = queue.Queue()
    ua = UserAgent()

    def __init__(self, date_last, date_to):
        self.date_last = date_last
        self.date_to = date_to
        self.ids_set = set()
        self.proxy = random.choice(PROXIES_LIST)
        self.roles = None
        self.count = 0
        self.count_errors = 0
        self.headers = {"User-Agent": self.ua.random}


    def api_req(self, page, date_from, date_to,retry=5):
        params = {
            'per_page': 100,
            'page': page,
            'professional_role': self.roles,
            'date_from': f'{date_from.isoformat()}',
            'date_to': f'{date_to.isoformat()}'}
        req = None
        try:
            req = requests.get(URL, params, proxies=self.proxy, headers=self.headers, timeout=5)
            req.raise_for_status()

        except requests.exceptions.ConnectionError as e:
            logger.info(f'{e}!!!!!!! retry {retry} proxy {self.proxy}')
            time.sleep(15)
            self.proxy = random.choice(PROXIES_LIST)
            return self.api_req(page, date_from, date_to)
        except Exception as err:

            time.sleep(4)
            if retry:
                logger.info(f'{err}. retry {retry} proxy {self.proxy}')
                return self.api_req(page, date_from, date_to, retry=(retry - 1))
            else:
                self.proxy = random.choice(PROXIES_LIST)
                return self.api_req(page, date_from, date_to)
        else:

            self.count += 1
            data = req.content.decode()
            data = json.loads(data)
            if data['items'] == []:
                time.sleep(1)
                data = self.api_req(page, date_from, date_to)
                print('ПРОБУЮ ЕЩЁ РАЗ')
            return data
        finally:
            if req != None:
                req.close()
                if self.count >= 10:
                    time.sleep(1)
                    self.count = 0

    def get_time_step(self, date_left, date_right):
        if date_left < 0:
            date_left = 0
        data = self.api_req(0, self.convert_seconds_in_date(date_left), self.convert_seconds_in_date(date_right))
        if data['found'] < DEFAULT_MAX_REC_RETURNED:
            print('Хватает')
            self.queue_a.put(
                [self.convert_seconds_in_date(date_left), self.convert_seconds_in_date(date_right)])
        else:
            print('много')
            while date_right != date_left:
                self.queue_a.put([self.convert_seconds_in_date(date_right - DEFAULT_MIN_STEP_SIZE),
                                  self.convert_seconds_in_date(date_right)])
                date_right -= DEFAULT_MIN_STEP_SIZE
        return date_left



    def convert_date_in_seconds(self, date):
        return (date - self.date_last).total_seconds()

    def convert_seconds_in_date(self, seconds):
        return self.date_last + timedelta(days=seconds / (24 * 60 * 60))

    def add_ids_in_set(self, data):
        for i in data['items']:
            self.ids_set.add(i['id'])

    def make_req_ids(self, id, retry=5):
        url = f'{URL}/{id}'
        req = None
        try:
            req = requests.get(url, proxies=self.proxy, headers=self.headers, timeout=5)
            req.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            logger.info(f'{e}!!!!!!! retry {retry} proxy {self.proxy}')

            time.sleep(20)
            self.proxy = random.choice(PROXIES_LIST)
            return self.make_req_ids(id)
        except Exception as err:
            self.count_errors += 1
            if self.count_errors > 5:
                self.count_errors = 0
                return None

            time.sleep(self.count_errors * 2)
            if retry:
                time.sleep(20)
                logger.info(f'{err}. retry {retry} proxy {self.proxy}')
                return self.make_req_ids(id, retry=(retry - 1))
            else:
                self.proxy = random.choice(PROXIES_LIST)
                return self.make_req_ids(id)
        else:
            self.count_errors = 0
            self.count += 1
            data = req.content.decode()
            print(f'Запрос успешный {self.count}')
            return data
        finally:
            if req != None:
                req.close()
                if self.count % 10 == 0:
                    time.sleep(1)
                    # self.count = 0

    def process_data_from_queue(self):
            try:
                conn = psycopg2.connect(database='HeadHunter', user='postgres', host='localhost', port='5432',
                                        password='2280')
                cur = conn.cursor()
                while not self.queue_b.empty():
                    data = self.queue_b.get()
                    print('добавил элемент')

                    vacancies_id = json.loads(data)['id']

                    sql = '''INSERT INTO vacancies (vacancies_id, data_jsonb) VALUES (%s, %s)'''
                    try:
                        cur.execute(sql, (vacancies_id, str(data)))
                        conn.commit()
                    except IntegrityError:
                        conn.rollback()
                        print(f'Дубликат элемента {vacancies_id}')

            except Exception as err:
                print(err)
            finally:
                cur.close()
                conn.close()
                logger.info('Закончил добавлять в базу____!')


    def run(self):
        logger.info(f'Запуск парсера. Временной интервал: От {self.date_to} --> до --> {self.date_last}')
        self.date_to = self.convert_date_in_seconds(self.date_to)
        while self.date_to != 0:
            next_date = self.get_time_step(self.date_to - DEFAULT_MAX_STEP_SIZE, self.date_to)
            print(self.date_to)
            self.date_to = next_date

        logger.info('процесс начал парсить ids')

        while not self.queue_a.empty():
            date_step = self.queue_a.get()
            for page in range(20):
                data = self.api_req(page, date_step[0], date_step[1])
                if (data['pages'] - page) <= 1:
                    break
                self.add_ids_in_set(data)

        logger.info('процесс закончил парсить ids')
        print(self.ids_set)
        for id in self.ids_set:
            data = self.make_req_ids(id)
            if data == None:
                continue
            self.queue_b.put(data)
            if self.count == 1000:
                logger.info('Добавляю в базу')
                self.process_data_from_queue()
                self.count =0

        logger.info('процесс закончил свою работу')

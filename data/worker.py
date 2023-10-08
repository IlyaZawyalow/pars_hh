import json
import requests
import time
from datetime import datetime, timedelta
from loguru import logger
import queue
import asyncio
import psycopg2

ID_ROLES_LIST = ["156", "160", "10", "12", "150", "25", "165", "34", "36", "73", "155", "96", "164",
                 "104", "157", "107", "112", "113", "148", "114", "116", "121", "124", "125", "126"]
DEFAULT_MAX_REC_RETURNED = 2000
URL = 'https://api.hh.ru/vacancies'
DEFAULT_MAX_STEP_SIZE = 60 * 60


class Warker:
    ids_set = set()
    queue_a = queue.Queue()
    event = asyncio.Event()

    def __init__(self, date_last, date_to):
        self.date_last = date_last
        self.date_to = date_to

    def api_req(self, page, date_from, date_to, retry=10):
        params = {
            'per_page': 100,
            'page': page,
            'date_from': f'{date_from.isoformat()}',
            'date_to': f'{date_to.isoformat()}'}
        try:
            req = requests.get(URL, params)
            req.raise_for_status()
        except Exception as err:
            if retry:
                time.sleep(3)
                logger.info(f'Error 403. retry {retry}')
                return self.api_req(page, date_from, date_to, retry=(retry - 1))
            else:
                raise
        else:
            data = req.content.decode()
            data = json.loads(data)
            if data['items'] == []:
                data = self.api_req(page, date_from, date_to)
                print('ПРОБУЮ ЕЩЁ РАЗ')
            return data
        finally:
            req.close()

    def get_time_step(self, date_left, date_right):
        if date_left < 0:
            date_left = 0
        if date_left == self.date_to - DEFAULT_MAX_STEP_SIZE and date_right == self.date_to or date_left == 0:
            data = self.api_req(0, self.convert_seconds_in_date(date_left), self.convert_seconds_in_date(date_right))
            if data['found'] < DEFAULT_MAX_REC_RETURNED:
                return date_left, data['pages']

        mid = (date_right + date_left) / 2
        data = self.api_req(0, self.convert_seconds_in_date(mid), self.convert_seconds_in_date(self.date_to))
        if data['found'] > DEFAULT_MAX_REC_RETURNED:
            return self.get_time_step(mid, date_right)
        return mid, data['pages']

    def convert_date_in_seconds(self, date):
        return (date - self.date_last).total_seconds()

    def convert_seconds_in_date(self, seconds):
        return self.date_last + timedelta(days=seconds / (24 * 60 * 60))

    def add_ids_in_set(self, data):
        for i in data['items']:
            self.ids_set.add(i['id'])

    def make_req_ids(self, id, retry=10):
        url = f'{URL}/{id}'
        try:
            req = requests.get(url)
            req.raise_for_status()
        except Exception as err:
            if retry:
                time.sleep(20)
                logger.info(f'Error 403. retry {retry}')
                return self.make_req_ids(id, retry=(retry - 1))
            else:
                raise
        else:
            data = req.content.decode()
            data = json.loads(data)

            print('Запрос успешный')
            # with open(f'../../venv/vakansAreas/adreses_{id}.json', 'w') as file:
            #     json.dump(data, file, indent=4, ensure_ascii=False)


            return data
        finally:
            req.close()
            # time.sleep(0.25)

    async def add_in_queue(self):
        for id in self.ids_set:
            # Отправляем запрос и добавляем данные в очередь
            self.queue_a.put(self.make_req_ids(id))
            #Говорим, что появилсь новые данные
            self.event.set()
            await asyncio.sleep(0.1)

        self.queue_a.put(None)
        self.event.set()


    async def process_data_from_queue(self):
        while True:
            await self.event.wait()

            data = self.queue_a.get()

            if data is None:
                break


            id = data['id']
            name = data['name']
            print(id, name)
            try:
                conn = psycopg2.connect(database='test_pars', user='postgres', host='localhost', port='5432',
                                        password='2280')
                cur = conn.cursor()
                sql = '''INSERT INTO vac_ids (id_vac, discr) VALUES (%s, %s)'''
                cur.execute(sql, (id, name))
                conn.commit()
            except Exception as err:
                print(err)
            finally:
                cur.close()
                conn.close()
                self.event.clear()




    def run(self):
        logger.info(f'Запуск парсера. Временной интервал: От {self.date_to} --> до --> {self.date_last}')
        self.date_to = self.convert_date_in_seconds(self.date_to)
        while self.date_to != 0:
            next_date, pages = self.get_time_step(self.date_to - DEFAULT_MAX_STEP_SIZE, self.date_to)
            print(f'Найдены следующая дата {next_date}и кол-во страниц {pages}')
            for page in range(pages):
                print(f'Страница номер {page}')
                data = self.api_req(page, self.convert_seconds_in_date(next_date),
                                    self.convert_seconds_in_date(self.date_to))
                self.add_ids_in_set(data)
            self.date_to = next_date


        logger.info(f'Парсинг id вакансий закончен!')

        with open(f'result.txt', 'w') as file:
            file.write(str(self.ids_set))
        # print(len(self.ids_set))


        # Добавляем данные в очередь
        # for id in self.ids_set:
        #     self.queue_a.put(self.make_req_ids(id))

        # Добавляю в базу данных
        # self.process_data_from_queue()


        #
        # while not self.queue_a.empty():
        #     print(self.queue_a.get())

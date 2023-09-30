import json
import os
import requests
import time
from datetime import datetime, timedelta


def make_request(url, area, page, id_roles_list, date_from, date_to):
    params = {
        'area': area,
        'per_page': 100,
        'page': page,
        'professional_role': id_roles_list,
        'date_from': f'{date_from.isoformat()}',
        'date_to': f'{date_to.isoformat()}'}
    req = requests.get(url, params)
    data = req.content.decode()
    data = json.loads(data)
    req.close()
    time.sleep(1)
    return data

def get_vacancies_list(area, page, id_roles_list, next_date):
    url = 'https://api.hh.ru/vacancies'
    data = make_request(url, area, page, id_roles_list, next_date, date_to)
    with open(f'data/{area}_{page}_{date_to}.json', 'w') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    time.sleep(1)


def get_time_step(area, id_roles_list, date_left, date_right):
    if date_left == date_last and date_right == date_to:
        url = 'https://api.hh.ru/vacancies'
        data = make_request(url, area, 0, id_roles_list, date_left, date_to)
        if data['found'] < 2000:
            return date_left, data['pages']

    step = (date_right - date_left) / timedelta(days=2)

    mid = date_right - timedelta(days=step)

    url = 'https://api.hh.ru/vacancies'
    data = make_request(url, area,0, id_roles_list, mid, date_to)
    time.sleep(0.5)
    if data['found'] > 2000:
        return get_time_step(area, id_roles_list, mid, date_right)
    elif data['found'] < 1800:
        return get_time_step(area, id_roles_list, date_left, mid)

    return mid, data['pages']



id_roles_list = ["156", "160", "10", "12", "150", "25", "165", "34", "36", "73", "155", "96", "164", "104", "157",
                 "107", "112", "113", "148", "114", "116", "121", "124", "125", "126"]
date_to = datetime.now()
date_last = date_to - timedelta(days=0.5)
all_areas_list = list(range(1, 2))

t0 = time.time()
for area in all_areas_list:
    while date_to != date_last:
        next_date, page_count = get_time_step(area, id_roles_list, date_last, date_to)
        for page in range(page_count):
            get_vacancies_list(area, page, id_roles_list, next_date)
        print(f'От {date_to},---->До {next_date}')
        date_to = next_date

print(time.time() - t0)


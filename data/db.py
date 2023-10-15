import psycopg2
import os
import json
import requests


DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'headhunter'
DB_USER = 'postgres'
DB_PASSWORD = 2280



class DataBase:
    def __init__(self, db_name):
        self.db_name = db_name

    def create_db(self, data):
        try:
            conn = psycopg2.connect(database='hh', user='postgres', host='localhost', port='5432',
                                    password='2280')
            cur = conn.cursor()



            #
            #
            sql = f'''INSERT INTO employer (employer_id, name, url, alternate_url, vacancies_url, trusted) VALUES (%s, %s,%s, %s,%s, %s)'''
            cur.execute(sql, (data['employer']['id'], data['employer']['name'], data['employer']['url'], data['employer']['alternate_url'],
                              data['employer']['vacancies_url'], data['employer']['trusted']))
            sql = f'''INSERT INTO vacancies (vacancies_id, billing_type, name, response_letter_required, salary_from, salary_to, salary_currency, salary_gross, type, allow_messages, experience, schedule, employment, department, description, accept_handicapped, accept_kids,archived, code, hidden, quick_responses_allowed, accept_incomplete_resumes, published_at, created_at, negotiations_url, suitable_resumes_url, apply_alternate_url, has_test, alternate_url, accept_temporary, employer_id, area_id) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s, %s, %s)'''
            cur.execute(sql, (data['id'], data['billing_type']['name'], data['name'], data['response_letter_required'],
                              data['salary']['from'], data['salary']['to'], data['salary']['currency'],
                              data['salary']['gross'], data['type']['name'], data['allow_messages'],
                              data['experience']['name'], data['schedule']['name'], data['employment']['name'],
                              data['department'], data['description'], data['accept_handicapped'], data['accept_kids'],
                              data['archived'], data['code'], data['hidden'], data['quick_responses_allowed'],
                              data['accept_incomplete_resumes'], data['published_at'], data['created_at'],
                              data['negotiations_url'], data['suitable_resumes_url'], data['apply_alternate_url'],
                              data['has_test'], data['alternate_url'], data['accept_temporary'], data['employer']['id'], data['area']['id']))

            try:
                sql = f'''INSERT INTO addresses (vacancies_id, area_id, street, building, lat, lng, description, raw, metro_station_id) VALUES (%s, %s,%s, %s,%s, %s, %s, %s, %s)'''
                cur.execute(sql,
                            (data['id'], data['area']['id'], data['address']['street'], data['address']['building'],
                             data['address']['lat'], data['address']['lng'], data['address']['description'],
                             data['address']['raw'], data['address']['metro']['station_id']))
            except Exception as err:
                print(err)

            conn.commit()
        except Exception as err:
            print(err)
        finally:
            cur.close()
            conn.close()

    def add_areas(self, data):
        try:
            conn = psycopg2.connect(database='hh', user='postgres', host='localhost', port='5432',
                                    password='2280')
            cur = conn.cursor()
            for i in data[0]['areas']:
                province_id = i['id']
                province_name = i['name']
                sql = f'''INSERT INTO provinces (province_id, province_name) VALUES (%s, %s)'''
                cur.execute(sql, (province_id, province_name))
                sql = f'''INSERT INTO areas (area_id, area_name, province_id) VALUES (%s, %s, %s)'''
                cur.execute(sql, (province_id, province_name, province_id))
                for j in i['areas']:
                    area_id = j['id']
                    area_name = j['name']
                    province_id = j['parent_id']
                    sql = f'''INSERT INTO areas (area_id, area_name, province_id) VALUES (%s, %s, %s)'''
                    cur.execute(sql, (area_id, area_name, province_id))
            conn.commit()
        except Exception as err:
            print(err)
        finally:
            cur.close()
            conn.close()

    def add_metro(self, data):
        try:
            conn = psycopg2.connect(database='hh', user='postgres', host='localhost', port='5432',
                                    password='2280')
            cur = conn.cursor()
            for i in range(7):
                for j in data[i]['lines']:
                    line_id = j['id']
                    hex_color = j['hex_color']
                    name = j['name']
                    sql = f'''INSERT INTO metro_lines (line_id, line_name, hex_color) VALUES (%s, %s, %s)'''
                    cur.execute(sql, (line_id, name, hex_color))
                    for k in j['stations']:
                        station_id = k['id']
                        station_name = k['name']
                        lat = k['lat']
                        lng = k['lng']
                        metro_order = k['order']
                        sql = f'''INSERT INTO metro_stations (station_id, station_name, line_id, lat, lng, metro_order) VALUES (%s, %s, %s, %s, %s, %s)'''
                        cur.execute(sql, (station_id, station_name, line_id, lat, lng, metro_order))
            conn.commit()
        except Exception as err:
            print(err)
        finally:
            cur.close()
            conn.close()



db = DataBase('HeadHunter')
with open('../../venv/vakansAreas/71628297.json') as file:
    data = json.load(file)
db.create_db(data)
# 71628297

req = requests.get('https://api.hh.ru/areas')
data = req.content.decode()
data = json.loads(data)
with open('areas.json', 'w') as file:
    json.dump(data, file, indent=4, ensure_ascii=False)
# db.add_areas(data)


req = requests.get('https://api.hh.ru/metro')
data = req.content.decode()
data = json.loads(data)
with open('metro.json', 'w') as file:
    json.dump(data, file, indent=4, ensure_ascii=False)
# db.add_metro(data)


        #
        #     conn = psycopg2.connect(database='test_pars', user='postgres', host='localhost', port='5432',password='2280')
        #     cur = conn.cursor()
        #     sql = '''INSERT INTO vac_ids (id_vac, discr) VALUES (%s, %s)'''
        #     cur.execute(sql, (id, name))
        #     conn.commit()
        # except IntegrityError:
        #     print(f'Дубликат элемента {id}')
        # except Exception as err:
        #     print(err)
        # finally:
        #     cur.close()
        #     conn.close()

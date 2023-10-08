def add_to_db():
    try:
        conn = psycopg2.connect(database='test', user='postgres', host='localhost', port='5432',
                                password='2280')
        cur = conn.cursor()
        cur.execute(''' ''', (value,))
        conn.commit()
    except Exception as err:
        print(err)
    finally:
        cur.close()
        conn.close()
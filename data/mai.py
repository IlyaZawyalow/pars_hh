import worker
import time
from datetime import datetime, timedelta
import multiprocessing

PROXIES_LIST = [None]


def worker_func(q, time):
    pr = worker.Worker(q, *time)
    pr.run(q)


def get_date(proc_count, timedelta_in_seconds):
    list_time = []
    now = datetime.now()
    date_to = (now - timedelta(minutes=now.minute % 5)).replace(second=0, microsecond=0)
    for i in range(proc_count):
        list_time.append([date_to - timedelta(seconds=timedelta_in_seconds), date_to])
        date_to -= timedelta(seconds=timedelta_in_seconds)
    return list_time


if __name__ == '__main__':
    t0 = time.time()

    manager = multiprocessing.Manager()
    q = manager.Queue()
    for i in PROXIES_LIST:
        q.put(i)

    pool = multiprocessing.Pool()
    list_time = get_date(6, 15000)
    task_arge = [(q, t) for t in list_time]

    pool.starmap(worker_func, task_arge)
    pool.close()
    pool.join()

    print(time.time() - t0)

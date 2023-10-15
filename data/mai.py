import worker
import time
from datetime import datetime, timedelta
import multiprocessing


def worker_func(time):
    pr = worker.Worker(*time)
    pr.run()

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

    pool = multiprocessing.Pool()

    list_time = get_date(7, 4800)
    task_arge = [(t,) for t in list_time]

    pool.starmap(worker_func, task_arge)
    pool.close()
    pool.join()

    print(time.time()-t0)












































# def wr(time, q):
#     pr = worker.Worker(*time)
#     pr.run(q)
#
# # def worker_wrapper(args):
# #     q, task = args
# #     result = warker(task, q)
#
# def get_date(proc_count, timedelta_in_seconds):
#     list_time = []
#     date_to = datetime.now()
#     for i in range(proc_count):
#         list_time.append([date_to, date_to - timedelta(seconds=timedelta_in_seconds)])
#         date_to -= timedelta(seconds=timedelta_in_seconds)
#     return list_time
# # def main():
# #     t0 = time.time()
# #     q2 = multiprocessing.Queue()
#
#
#
#     # pr1 = worker.Worker(date_last, date_to)
#     # pr2 = worker.Worker(date_last2, date_to2)
#     # pr3 = worker.Worker(date_last3, date_to3)
#     # pr4 = worker.Worker(date_last4, date_to4)
#     # pr5 = worker.Worker(date_last5, date_to5)
#     # pr6 = worker.Worker(date_last6, date_to6)
#     # pr7 = worker.Worker(date_last7, date_to7)
#     # pr8 = worker.Worker(date_last8, date_to8)
#     #
#     # proc1 = multiprocessing.Process(target=pr1.run, name='proc-1', args=(q,))
#     # proc2 = multiprocessing.Process(target=pr2.run, name='proc-2', args=(q,))
#     # proc3 = multiprocessing.Process(target=pr3.run, name='proc-3', args=(q,))
#     # proc4 = multiprocessing.Process(target=pr4.run, name='proc-4', args=(q,))
#     # proc5 = multiprocessing.Process(target=pr5.run, name='proc-5', args=(q,))
#     # proc6 = multiprocessing.Process(target=pr6.run, name='proc-6', args=(q,))
#     # proc7 = multiprocessing.Process(target=pr7.run, name='proc-7', args=(q,))
#     # proc8 = multiprocessing.Process(target=pr8.run, name='proc-8', args=(q,))
#     # proc1.start()
#     # proc2.start()
#     # proc3.start()
#     # proc4.start()
#     # proc5.start()
#     # proc6.start()
#     # proc7.start()
#     # proc8.start()
#     #
#     # process = [proc1, proc2, proc3, proc4, proc5, proc6, proc7, proc8]
#     # while any(proc.is_alive() for proc in process):
#     #     s = [proc.is_alive() for proc in process]
#     #     # print(s)
#     #     time.sleep(1)
#     #
#     # proc1 = multiprocessing.Process(target=pr1.next_step, name='proc-1', args=(q, q2))
#     # proc2 = multiprocessing.Process(target=pr2.next_step, name='proc-2', args=(q, q2))
#     # proc3 = multiprocessing.Process(target=pr3.next_step, name='proc-3', args=(q, q2))
#     # proc4 = multiprocessing.Process(target=pr4.next_step, name='proc-4', args=(q, q2))
#     # proc5 = multiprocessing.Process(target=pr5.next_step, name='proc-5', args=(q, q2))
#     # proc6 = multiprocessing.Process(target=pr6.next_step, name='proc-6', args=(q, q2))
#     # proc7 = multiprocessing.Process(target=pr7.next_step, name='proc-7', args=(q, q2))
#     # proc8 = multiprocessing.Process(target=pr8.next_step, name='proc-8', args=(q, q2))
#     # proc1.start()
#     # proc2.start()
#     # proc3.start()
#     # proc4.start()
#     # proc5.start()
#     # proc6.start()
#     # proc7.start()
#     # proc8.start()
#     #
#     #
#     # print('sssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss')
#     # process = [proc1, proc2, proc3, proc4, proc5, proc6, proc7, proc8]
#     # while any(proc.is_alive() for proc in process):
#     #     s = [proc.is_alive() for proc in process]
#     #     print(s)
#     #     time.sleep(1)
#
#     #
#     # proc1.join(timeout=60)
#     #
#     # proc2.join(timeout=60)
#     #
#     # proc3.join(timeout=60)
#     #
#     #
#     #
#     #
#     #
#     # itog_set = set()
#     # while not q2.empty():
#     #     itog_set = itog_set.union(q2.get())
#     # print(len(itog_set))
#     # print(time.time()- t0)
#
#
#     # for i in itog_set:
#     #     q.put(i)
#
#
#     # #
#     # # proc1 = multiprocessing.Process(target=pr1.add_data_in_queue, name='proc-1', args=(q,q2))
#     #
#     # proc1 = multiprocessing.Process(target=pr1.add_data_in_queue, name='proc-1', args=(q,q2))
#     # proc2 = multiprocessing.Process(target=pr2.add_data_in_queue, name='proc-2', args=(q,q2))
#     # proc1.start()
#     # proc2.start()
#     # proc1.join()
#     # proc2.join()
#     # pr_list = []
#     # for i in range(2):
#     #     pr = multiprocessing.Process(target=pr1.add_data_in_queue, args=(q, q2))
#     #     pr_list.append(pr)
#     #     pr.start()
#     # for j in pr_list:
#     #     j.join()
#
#     # pr2.process_data_from_queue(q2)
#
#
#
#
#
#     # list_pr = []
#     # for i in range(2):
#     #     pr = multiprocessing.Process(target=pr1.ggt, args=(q,))
#     #     list_pr.append(pr)
#     #     pr.start()
#     # for j in list_pr:
#     #     j.join()
#
#     # itog = set()
#     # while not q.empty():
#     #     itog = itog.union(q.get())
#     # print(itog)
#
#
#
#     # t0 = time.time()
#     # task1 = asyncio.create_task(pr.add_in_queue())
#     # task2 = asyncio.create_task(pr.process_data_from_queue())
#     # await asyncio.gather(task1, task2)
#     # print(time.time() - t0)
#
#
# if __name__ == '__main__':
#     manager = multiprocessing.Manager()
#     q = manager.Queue()
#     pool = multiprocessing.Pool()
#     list_time = get_date(4, 20000)
#     task_arge = [(q, t) for t in list_time]
#
#     pool.starmap(wr, task_arge)
#     pool.close()
#     pool.join()
#     results = []
#     while not q.empty():
#         results.append(q.get())
#     print(results)
#
#     # with multiprocessing.Pool(multiprocessing.cpu_count() * 3) as pool:
#     #     pool.starmap(wr, task_arge)
#         # for t in list_time:
#         #     pool.apply_async(wr, args=(t, q))
#
#         # # pool.map(worker_wrapper, task_arge)
#         # pool.close()
#         # pool.join()
#     results = []
#     while not q.empty():
#         results.append(q.get())
#     print(results)
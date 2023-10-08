import worker
import time
from datetime import datetime, timedelta
import asyncio
import multiprocessing




async def main():
    t0 = time.time()
    q = multiprocessing.Queue()
    q2 = multiprocessing.Queue()





    date_to = datetime.now()
    date_last = date_to - timedelta(seconds=2000)
    date_to2 = date_last
    date_last2 = date_to2 - timedelta(seconds=2000)

    pr1 = worker.Warker(date_last, date_to)
    pr2 = worker.Warker(date_last2, date_to2)


    proc1 = multiprocessing.Process(target=pr1.run, name='proc-1', args=(q,))
    proc2 = multiprocessing.Process(target=pr2.run, name='proc-2', args=(q,))
    proc1.start()
    proc2.start()
    proc1.join()
    proc2.join()
    print(time.time()- t0)

    list_pr = []
    for i in range(2):
        pr = multiprocessing.Process(target=pr1.ggt, args=(q,))
        list_pr.append(pr)
        pr.start()
    for j in list_pr:
        j.join()

    # itog = set()
    # while not q.empty():
    #     itog = itog.union(q.get())
    # print(itog)



    # t0 = time.time()
    # task1 = asyncio.create_task(pr.add_in_queue())
    # task2 = asyncio.create_task(pr.process_data_from_queue())
    # await asyncio.gather(task1, task2)
    # print(time.time() - t0)


if __name__ == '__main__':
    asyncio.run(main())
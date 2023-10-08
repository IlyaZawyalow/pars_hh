import worker
import time
from datetime import datetime, timedelta
import asyncio
import multiprocessing

async def main():
    date_to = datetime.now()
    date_last = date_to - timedelta(seconds=5000)
    pr = worker.Warker(date_last, date_to)
    # pr.run()

    pr1 = multiprocessing.Process(target=pr.run, name='proc-1')
    pr1.start()
    pr1.join()

    # t0 = time.time()
    # task1 = asyncio.create_task(pr.add_in_queue())
    # task2 = asyncio.create_task(pr.process_data_from_queue())
    # await asyncio.gather(task1, task2)
    # print(time.time() - t0)


if __name__ == '__main__':
    asyncio.run(main())
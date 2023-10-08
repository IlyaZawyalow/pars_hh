import worker
import time
from datetime import datetime, timedelta
import asyncio

async def main():
    date_to = datetime.now()
    date_last = date_to - timedelta(seconds=500)
    pr = worker.Warker(date_last, date_to)

    pr.run()
    t0 = time.time()
    task1 = asyncio.create_task(pr.add_in_queue())
    task2 = asyncio.create_task(pr.process_data_from_queue())
    await asyncio.gather(task1, task2)
    print(time.time() - t0)


if __name__ == '__main__':
    asyncio.run(main())
    # date_to = datetime.now()
    # date_last = date_to - timedelta(seconds=300)
    # pr = worker.Warker(date_last, date_to)
    # t0 = time.time()
    # pr.run()


    #
    # event_loop = asyncio.get_event_loop()
    # event_loop.create_task(pr.add_in_queue())
    # event_loop.create_task(pr.process_data_from_queue())
    # event_loop.run_forever()
    #
    # print(time.time() - t0)
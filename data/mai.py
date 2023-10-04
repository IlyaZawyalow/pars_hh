import worker
import json
import os
import requests
import time
from datetime import datetime, timedelta

date_to = datetime.now()
date_last = date_to - timedelta(seconds=3000)
pr = worker.Warker(date_last, date_to)
t0 = time.time()
pr.run()
print(time.time() - t0)
import worker
import json
import os
import requests
import time
from datetime import datetime, timedelta
date_to = datetime.now()
date_last = date_to - timedelta(days=30)
pr = worker.Warker(date_last, date_to, 0, None)
pr.run()

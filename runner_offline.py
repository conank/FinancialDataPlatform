from utils import *
from jobs_offline import *
import logging


# Initiate Logger
logger = getLogger(name=__name__, handlers=[{"type": "stream"}])

# Insert all the historical price data #
# Get all stock codes
def appendHistDailyPrice():
    mongodb = MongoDb()
    mongodb.setCollection(daily_price_mongodb, daily_price_mongocol)
    logger.info("Get all stock codes")
    codes = mongodb.findDistinct("code")
    job_info = {"key": "code", "val": codes, "options": {"start": "1990-01-01", "interval": 5}}
    job_tracker = JobTracker(initHistDailyPrice, job_info=job_info)
    res = job_tracker.start()
    logger.info("Job running result: ")
    logger.info(res)

# Jobs to run
appendHistDailyPrice()
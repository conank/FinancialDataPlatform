from utils import *
import tushare as ts
from jobs import *
import logging


# Instantiate logger
log_handlers = [{"type": "stream"}]
logger = getLogger(name=__name__, handlers = log_handlers)

# Insert all the historical price data #
@log(logger)
def initHistDailyPrice(*args, **kwargs):
    code = kwargs["key_val"]
    start = kwargs["options"]["start"]
    mongodb = MongoDb()
    mongodb.setCollection(daily_price_mongodb, daily_price_mongocol)
    name = mongodb.findOne({"code": code}, fields=["name"])["name"]
    logger.info("Get daily price for: %s, %s", code, name)
    # Get all the price data for the code
    price_data = get_daily_price(code=code, name=name, start=start)
    # Write the price data into database
    mongodb.setCollection(daily_price_mongodb, daily_price_mongocol)
    mongodb.insert(price_data)
    mongodb.close()
        
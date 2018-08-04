from utils import *
import tushare as ts
from jobs import *
import logging


# Instantiate logger
log_handlers = [{"type": "stream"}]
logger = getLogger(name=__name__, handlers = log_handlers)


# Insert all the historical price data #
@log(logger)
def initHistDailyPrice(start=''):
    # Check if the database already exists in MongoDB, if so skip reading codes from daily_price 
    # Get all stock codes
    codes = findDistinctMongoDoc(daily_price_mongodb, daily_price_mongocol, "code")
    for code in codes:
        # Check if the code has been processed previously
        
        # Get stock name for the code
        name = findOneMongoDoc(daily_price_mongodb, daily_price_mongocol, {"code":code}, fields=["name"])["name"]
        logger.info("Get dialy price for: %s, %s", code, name)
        # Get all the price data for the code
        price_data = get_daily_price(code=code, name=name, start=start)
        # Write the price data into database
        mongodb = MongodDb()
        mongodb.setCollection(daily_price_mongodb, daily_price_mongocol)
        mongodb.insert(price_data)
        mongodb.close()
        
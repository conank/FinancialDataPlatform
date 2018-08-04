from settings import *
from utils import *
from jobs import *
import pymongo
import unittest
import sys

# Test email utils
# with open("logs/errors.log.2018-07-29_21", 'r') as file:
#     content = file.readlines()
# print(type(content))
# content = [x for x in content]
# send_email("shunrongshen637@live.com", "Test", "".join(content))

# Test logger
# import logging
# logger = Logger(global_logger_name)
# logger.info("Info")
# logger.error("Error")
# for handler in logger.handlers:
#     handler.doRollover()

# Test log wrapper
# logger = Logger(global_logger_name)
# @log(logging.getLogger(global_logger_name))
# def div():
#     return 1/0
# @log(logging.getLogger(global_logger_name))
# def div2():
#     return 4/2
# div()
# div2()

# Test get_daily_price()
# daily_prices = get_daily_price("600125", name="测试", start="1990-01-01")
# write2mongo(daily_prices, daily_price_mongodb, daily_price_mongocol)

# Test indexing method of MongoDb
# print(createMongoIdx("price_data", "daily", {"code": pymongo.ASCENDING, "date": pymongo.DESCENDING}))

class Test(unittest.TestCase):
    @unittest.skip("findDistinctMongoDoc")
    def test_findDistinctMongoDoc(self):
        res = findDistinctMongoDoc("price_data", "daily", "code")
        print(res)

    @unittest.skip("findOneMongoDoc")
    def test_findOneMongoDoc(self):
        res = findOneMongoDoc("price_data", "daily", {"code": "600125"}, ["name", "date"])
        print(res)


    @unittest.skip("getLogger")
    def test_getLogger(self):
        handlers = [{"type": "stream", 
                    "options": {"stream": sys.stdout} }]
        logger = getLogger("logger_test", handlers=handlers)
        print("")
        logger.info("Testing logger")

    def test_timezone(self):
        time = datetime.datetime.now()
        print("original time: ", time)
        time_utc = local2utc(time)
        print("UTC time: ", time_utc)
        time_local = utc2local(time_utc)
        print("local time: ", time_local)

if __name__=="__main__":
    unittest.main()


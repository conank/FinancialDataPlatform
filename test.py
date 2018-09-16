from settings import *
from utils import *
from jobs import *
from jobs_offline import *
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

    @unittest.skip("not testing")
    def test_mongodb_replace(self):
        mongodb = MongoDb()
        mongodb.setCollection(daily_price_mongodb, daily_price_mongocol)
        prev_data = list(mongodb.find({"code": "603991"}))
        print(prev_data)
        prev_data[0]["name"] = "test"
        mongodb.findOneAndReplace({"code": "603991"}, prev_data[0])
        res = mongodb.find({"$and": [{"code": "603991"}, {"name": "test"}]})
        print(res)

    @unittest.skip("Not testing")
    def test_initHistDailyPrice(self):
        mongodb = MongoDb()
        mongodb.setCollection(daily_price_mongodb, daily_price_mongocol)
        initHistDailyPrice(key_val="603939", options={"start": "1991-01-01"})
        res = mongodb.find(conditions={"code": "603939"})
        print(res)
        mongodb.close()

    @unittest.skip("Not testing")
    def test_jobTracker(self):
        mongodb = MongoDb()
        mongodb.setCollection(daily_price_mongodb, daily_price_mongocol)
        mongodb.delete({"code": {"$nin": ["603939", "603920", "603970"]}})
        job_info = {"key": "code", "val": ["603939", "603920"], "options": {"start": "1991-01-01"}}
        job_tracker = JobTracker(initHistDailyPrice, job_info=job_info)
        # Test job_tracker.exists
        self.assertTrue(job_tracker.jobExists(initHistDailyPrice))
        job_info = {"key": "code", "val": ["603939", "603920", "603970"], "options": {"start": "1991-01-01"}}
        # Test setKeyInfo
        job_tracker.setJobInfo(job_info)
        # Test success case for job_tracker
        res = job_tracker.start()
        self.assertEqual(res["status"], JobStatus.Finished.value)

    def test_backupVolume(self):
        sink = os.getcwd()
        res = backupVolume(source="mongodb", sink=sink)
        self.assertEqual(res.returncode, 0)

    def test_restoreVolume(self):
        source_folder = os.getcwd()
        res = restoreVolume(source_folder=source_folder, source_file="financial_data_2018-08-07.tar.gz", target="mongodb")
        self.assertEqual(res.returncode, 0)

    def test_recordStockClassifications(self):
        recordStockClassifications()

    def test_getFundamentals(self):
        getFundamental(2018, 1)

    def test_get_last_trade_day_price(self):
        get_last_trade_day_price(datetime.datetime.now())

if __name__=="__main__":
    unittest.main()


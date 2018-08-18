from utils import *
from settings import *
from models import *
import tushare as ts
import copy
import getpass

# Instantiate envs
logger = Logger(global_logger_name)

# Get the prices of all listed stocks on the last trade day
@log(logger)
def get_last_trade_day_price(trade_day):
    # Get all the data for latest trading date
    prices_df = ts.get_today_all()
    prices_df.rename(columns={"trade":"close"}, inplace=True) # Match the column name to the results returned by get_k_data
    num_stocks = prices_df.shape[0]
    prices_dict = prices_df.to_dict()
    prices_list = []

    # Repack the price data for insertion into the mongoDB
    for idx in range(num_stocks):
        daily_price = {} #copy.deepcopy(daily_price_template)
        for field in prices_dict.keys():
            daily_price[field] = prices_dict[field][idx]
        daily_price["date"] = datetime.datetime.utcfromtimestamp(trade_day.timestamp())
        daily_price["timeModified"] = datetime.datetime.utcnow()
        daily_price["timeCreated"] = datetime.datetime.utcnow()
        prices_list.append(daily_price)

    # Save the data to MongoDB
    mongodb = MongoDb()
    mongodb.setCollection(daily_price_mongodb, daily_price_mongocol)
    mongodb.insert(prices_list)
    mongodb.close()


# Get daily price info of the code in the period from start to end
# return: dict that contains the info
def get_daily_price(code, name='', start='', end='', index=False):
    data = ts.get_k_data(code=code, start=start, 
                          end=end, ktype='D', index=index, pause=0.1)
    prices = []
    data_dict = data.to_dict() 
    for idx in list(data.index.values):
        daily_price = {}
        for field in data_dict.keys():
            if field != "date":
                daily_price[field] = data_dict[field][idx]
            else:
                daily_price[field] = datetime.datetime.utcfromtimestamp(
                    str2datetime(data_dict[field][idx], "%Y-%m-%d").timestamp())
        daily_price["timeModified"] = datetime.datetime.utcnow()
        daily_price["timeCreated"] = datetime.datetime.utcnow()
        daily_price["code"] = code
        daily_price["name"] = name
        prices.append(daily_price)
    return prices


# Backup docker volume by first loading the volume of a container into another container, then zip it into a folder that linked to a local directory
# source: the container name that running on the volume that needs to be backup
# sink: the directory on the host machine to store the backup
# file: the name of the backup
@log(logger)
def backupVolume(source, sink, file=None):
    logger.info("User: %s", getpass.getuser())
    if "\\" in sink:
        sink += "\\" + "backup"
    else:
        sink += "/backup"
    if file is None:
        file = backup_default_file_prefix + "_" + datetime2str(datetime.datetime.now(), "%Y-%m-%d") + ".tar.gz"
    cmd = ["docker", "run", "--rm", "--volumes-from",
           source, "-v", sink + ":/backup", "ubuntu",
           "tar", "zcvf", "/backup/" + file, "/data/db"]
    print(" ".join(cmd))
    res = execShellCmd(cmd)
    return res
    

# Record various stock classifications using data from tushare every season
def obtainClassData(api, class_name, mongo_col):
    logger.info("Fetching %s data", class_name)
    data_df = api()
    if "c_name" in data_df.columns:
        data_df.rename(columns={"c_name": class_name}, inplace=True)
    data_dict = transformDfToDict(data_df)
    data = {"classification": data_dict,
            "date": datetime.datetime.utcnow()
            }

    # Write to mongodb
    mongodb = MongoDb()
    mongodb.setCollection(stock_class_mongodb, mongo_col)
    mongodb.insert(data)
    mongodb.close()

@log(logger)
def recordStockClassifications():
    # Industry
    class_name = "industry"
    obtainClassData(ts.get_industry_classified, class_name, class_name)

    # Concept
    class_name = "concept"
    obtainClassData(ts.get_concept_classified, class_name, class_name)

    # Area
    class_name = "area"
    obtainClassData(ts.get_area_classified, class_name, class_name)

    # SME (中小板)
    class_name = "sme"
    obtainClassData(ts.get_sme_classified, class_name, class_name)

    # 创业板
    class_name = "gem"
    obtainClassData(ts.get_gem_classified, class_name, class_name)

    # ST (风险警示)
    class_name = "st"
    obtainClassData(ts.get_st_classified, class_name, class_name)

    # hs300 沪深300成份及权重
    class_name = "hs300"
    obtainClassData(ts.get_hs300s, class_name, class_name)

    # 上证50成份股
    class_name = "sz50s"
    obtainClassData(ts.get_sz50s, class_name, class_name)

    # 中证500成份股
    class_name = "zz500s"
    obtainClassData(ts.get_zz500s, class_name, class_name)

    # 终止上市股票列表
    class_name = "terminated"
    obtainClassData(ts.get_terminated, class_name, class_name)
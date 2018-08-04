from utils import *
from settings import *
from models import *
import tushare as ts
import copy

# Instantiate envs
logger = Logger(global_logger_name)

# Get the prices of all listed stocks on the last trade day
@log(logger)
def get_last_trade_day_price():
    # Get all the data for latest trading date
    prices_df = ts.get_today_all()
    prices_df.rename(columns={"trade":"close"}, inplace=True) # Match the column name to the results returned by get_k_data
    num_stocks = prices_df.shape[0]
    prices_dict = prices_df.to_dict()
    prices_list = []

    # Get the latest trading date from get the all the trading day within a 20-day period from today
    date_20days_ago = datetime.datetime.now() - datetime.timedelta(days=20)
    latest_trade_date = ts.get_h_data("601318", start=datetime.datetime.strftime(date_20days_ago, "%Y-%m-%d")) \
                        .index[0] # Use PinAn as a proxy to quote the historical data 

    # Repack the price data for insertion into the mongoDB
    for idx in range(num_stocks):
        daily_price = {} #copy.deepcopy(daily_price_template)
        for field in prices_dict.keys():
            daily_price[field] = prices_dict[field][idx]
        daily_price["date"] = datetime.datetime.utcfromtimestamp(latest_trade_date.timestamp())
        daily_price["timeModified"] = datetime.datetime.utcnow()
        daily_price["timeCreated"] = datetime.datetime.utcnow()
        prices_list.append(daily_price)

    # Save the data to MongoDB
    mongodb = MongodDb()
    mongodb.setCollection(daily_price_mongodb, daily_price_mongocol)
    mongodb.insert(prices_list)
    mongodb.close()


# Get daily price info of the code in the period from start to end
# return: dict that contains the info
def get_daily_price(code, name='', start='', end='', index=False):
    data = ts.get_k_data(code=code, start=start, 
                          end=end, ktype='D', index=index, pause=0.1)
    num_data = data.shape[0]
    prices = []
    data_dict = data.to_dict() 
    for idx in range(num_data):
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

    
    

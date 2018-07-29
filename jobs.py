from utils import *
from settings import *
from models import *
import tushare as ts
import copy

# Get the prices of all listed stocks on the last trade day
def get_last_trade_day_price():
    # Get all the data for latest trading date
    prices_df = ts.get_today_all()
    prices_df.rename(columns={"trade":"close"}, inplace=True) # Match the column name to the results returned by get_k_data
    numStocks = prices_df.shape[0]
    prices_dict = prices_df.to_dict()
    prices_list = []

    # Get the latest trading date from get the all the trading day within a 20-day period from today
    date_20days_ago = datetime.datetime.now() - datetime.timedelta(days=20)
    latest_trade_date = ts.get_h_data("601318", start=datetime.datetime.strftime(date_20days_ago, "%Y-%m-%d")) \
                        .index[0] # Use PinAn as a proxy to quote the historical data 

    # Repack the price data for insertion into the mongoDB
    for idx in range(numStocks):
        daily_price = {} #copy.deepcopy(daily_price_template)
        for field in prices_dict.keys():
            daily_price[field] = prices_dict[field][idx]
        daily_price["date"] = datetime.datetime.utcfromtimestamp(latest_trade_date.timestamp())
        daily_price["timeModified"] = datetime.datetime.utcnow()
        daily_price["timeCreated"] = datetime.datetime.utcnow()
        prices_list.append(daily_price)

    # Save the data to MongoDB
    write2mongo(prices_list, daily_price_mongodb, daily_price_mongocol)

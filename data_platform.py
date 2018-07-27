from pymongo import MongoClient
import data_model
import tushare as ts
import datetime
import copy

# Env settings
mongo_host = "localhost"
mongo_port = 27017
mongo_db = "price_data"
mongo_col = "daily"

# Get all the data for latest trading date
daily_data_df = ts.get_today_all()
numStocks = daily_data_df.shape[0]
daily_data_dict = daily_data_df.to_dict()
daily_data_list = []

# Get the latest trading date from get the all the trading day within a 20-day period from today
date_20days_ago = datetime.datetime.now() - datetime.timedelta(days=20)
latest_trade_date = ts.get_h_data("601318", start=datetime.datetime.strftime(date_20days_ago, "%Y-%m-%d")) \
                      .index[0] # Use PinAn as a proxy to quote the historical data 

# Repack the price data for insertion into the mongoDB
for idx in range(numStocks):
    daily_data = copy.deepcopy(data_model.daily_data_template)
    for field in daily_data_dict.keys():
        daily_data[field] = daily_data_dict[field][idx]
    daily_data["date"] = datetime.datetime.utcfromtimestamp(latest_trade_date.timestamp())
    daily_data["timeModified"] = datetime.datetime.utcnow()
    daily_data["timeCreated"] = datetime.datetime.utcnow()
    daily_data_list.append(daily_data)

# Establish connection with MongoDB
mongo_client = MongoClient(host=mongo_host, port=mongo_port)
mongo_conn = mongo_client[mongo_db][mongo_col]
insert_res = mongo_conn.insert_many(daily_data_list)
mongo_client.close()

#TODO Create indices for the collection if haven't been set yet

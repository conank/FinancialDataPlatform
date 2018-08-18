from utils import *
from jobs import *
import datetime
import tushare as ts

# Runner settings
debug = True
dates =  []
date_format = "%Y-%m-%d"

# Get today's date if no date is specified
if dates is None or len(dates) == 0:
    dates = [datetime2str(datetime.datetime.now(), date_format)]

# Setup env

for date in dates:
    today = str2datetime(date, date_format)
    # Run jobs for each trading date, assuming 
    # 1. The runner is launched after 21:00T+08:00 on every trading day
    # 2. No trading day is on Saturday or Sunday
    if isworkday(today) and not ts.is_holiday(datetime2str(today, date_format)):
        logger.info("Running daily jobs")
        get_last_trade_day_price(today)

    # Run weekly jobs on every Sunday
    if today.isoweekday() == 7:
        logger.info("Running weekly jobs")
        backupVolume(source=mongo_docker_name, sink=os.getcwd())

    # Run monthly jobs on the first day of each month
    if is_first_month_day(today):
        logger.info("Running monthly jobs")

    # Run seasonal jobs on the first day of each season
    if is_first_season_day(today):
        logger.info("Running seasonal jobs")
        recordStockClassifications()

# Rollover the logs everyweek
logger.info("Rollovering logs")
for handler in logger.handlers:
    handler.doRollover()

# Send email to notify about the completion of the runner
# msg = ["Runner triggered\n"]
# info_log = log_file_info + "." + datetime2str(datetime.datetime.now().date(), handler_rollover_suffix)
# if checkFileExists(info_log):
#     # Construct email body
#     with open(info_log) as log_file:
#         logs = log_file.readlines()
#     email_body = ["Runner triggered\n"] + logs
#     try:
#         send_email(notify_email_addr, "Financial Data Platform", "".join(email_body))
#     except:
#         logger.error("Failed to send_email", exc_info=1)
#         for handler in logger.handlers:
#             handler.doRollover()
# else:
#     logger.error("Info log file doesn't exits")
    

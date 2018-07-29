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

for date in dates:
    today = datetime.datetime.strptime(date, date_format)
    # Run jobs for each trading date, assuming the runner is launched after 21:00T+08:00 on every trading day
    if not ts.is_holiday(datetime2str(today, date_format)):
        logger.info("Running daily jobs")

    # Run weekly jobs on every Sunday
    if today.isoweekday() == 7:
        logger.info("Running weekly jobs")
        get_last_trade_day_price()


    # Run monthly jobs on the first day of each month
    if is_first_month_day(today):
        logger.info("Running monthly jobs")

    # Run seasonal jobs on the first day of each season
    if is_first_season_day(today):
        logger.info("Running seasonal jobs")

# Rollover the logs everyweek
logger.info("Rollovering logs")
for handler in logger.handlers:
    handler.doRollover()

# Send email to notify about the completion of the runner
send_email(notify_email_addr, "Financial Data Platform", "Runner triggered")
from utils import *
from jobs import *
import datetime
import tushare as ts

# Runner settings
debug = True
dates =  []
date_format = "%Y-%m-%d"

# TODO 1. Add logs to a file, 2. Send email to inform about successful implementation

# Get today's date if no date is specified
if dates is None or len(dates) == 0:
    dates = [datetime2str(datetime.datetime.now(), date_format)]

for date in dates:
    today = datetime.datetime.strptime(date, date_format)
    # Run jobs for each trading date, assuming the runner is launched after 21:00T+08:00 on every trading day
    if not ts.is_holiday(datetime2str(today, date_format)):
        print("Running daily jobs")

    # Run weekly jobs on every Sunday
    if today.isoweekday() == 7:
        print("Running weekly jobs")
        get_last_trade_day_price()

    # Run monthly jobs on the first day of each month
    if is_first_month_day(today):
        print("Running monthly jobs")

    # Run seasonal jobs on the first day of each season
    if is_first_season_day(today):
        print("Running seasonal jobs")
from utils import *
import datetime
import tushare as ts

# Runner settings
debug = True
dates =  ["2018-10-1"]
date_format = "%Y-%m-%d"

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

    # Run monthly jobs on the first day of each month
    if is_first_month_day(today):
        print("Running monthly jobs")

    # Run seasonal jobs on the first day of each season
    if is_first_season_day(today):
        print("Running seasonal jobs")
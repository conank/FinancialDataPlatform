import datetime

def datetime2str(date_time, datetime_format):
    return datetime.datetime.strftime(date_time, datetime_format)


def str2datetime(date_time, datetime_format):
    return datetime.datetime.strptime(date_time, datetime_format)

# Check if the given date is the first day of a month
# Input date is either a datetime object or a string in the format of "%Y-%m-%d"
def is_first_month_day(date):
    if type(date) in (type(datetime.datetime.now()), type(datetime.datetime.now().date())):
        if date.day == 1:
            return True
    elif type(date) == str:
        if date.split("-")[2] == 1:
            return True
    # Return False if the format of the input is unrecognized or the date is not the first day of a month
    return False


# Check if the given date is the first day of a season
# Input date is either a datetime object or a string in the format of "%Y-%m-%d"
def is_first_season_day(date):
    season_first_months = (1, 4, 7, 10) # The first months of the seasons
    if type(date) in (type(datetime.datetime.now()), type(datetime.datetime.now().date())):
        if date.month in season_first_months and date.day == 1:
            return True
    elif type(date) == str:
        if date.split("-")[1] in season_first_months and  date.split("-")[2] == 1:
            return True
    # Return False if the format of the input is unrecognized or the date is not the first day of a month
    return False
        
from pymongo import MongoClient
from settings import *
import datetime
import logging
from logging import handlers

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

# output data to mongoDB     
def write2mongo(data, mongodb, mongocol):
    mongo_client = MongoClient(host=mongo_host, port=mongo_port)
    mongo_conn = mongo_client[mongodb][mongocol]
    insert_res = mongo_conn.insert_many(data)
    mongo_client.close()
    return insert_res

def send_email(to_addrs, subject, body):
    # Import smtplib for the actual sending function
    import smtplib
    # Import the email modules we'll need
    from email.mime.text import MIMEText

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = smtp_username
    msg['To'] = to_addrs

    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    smtp_server = smtplib.SMTP(smtp_host)
    smtp_server.starttls()
    smtp_server.login(user=smtp_username, password=smtp_passwd)
    smtp_server.sendmail(smtp_username, msg["To"], msg.as_string())
    smtp_server.quit()
    print("finshed sending email")


def Logger(name=None, level=logging.INFO):
    if name is not None:
        logger = logging.getLogger(name=name)
    else:
        logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
    # Create file handler for info level msgs
    info_handler = logging.handlers.TimedRotatingFileHandler(filename=log_file_info, backupCount=180, encoding="utf8", delay=True)
    info_handler.setFormatter(formatter)
    info_handler.setLevel(logging.INFO)
    info_handler.suffix = handler_rollover_suffix
    logger.addHandler(info_handler)
    # Create file handler for error level msgs
    error_handler = logging.handlers.TimedRotatingFileHandler(filename=log_file_error, backupCount=180, encoding="utf8", delay=True)
    error_handler.setFormatter(formatter)
    error_handler.setLevel(logging.ERROR)
    error_handler.suffix = handler_rollover_suffix
    logger.addHandler(error_handler)
    return logger

def log(logger):
    def logFunction(func):
        logger.info("Executing %s", func.__name__)
        def execFunction(*args, **kwargs):
            try:
                exec_res = func(*args, **kwargs)
                logger.info("Completed executing %s", func.__name__)
                return exec_res
            except:
                logger.error("Exception thrown for " + func.__name__, exc_info=1)
        return execFunction
    return logFunction




        

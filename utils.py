from pymongo import MongoClient
from settings import *
import datetime
import logging
from logging import handlers
from pathlib import Path
import sys
import pytz

def datetime2str(date_time, datetime_format):
    return datetime.datetime.strftime(date_time, datetime_format)


def str2datetime(date_time, datetime_format):
    return datetime.datetime.strptime(date_time, datetime_format)

def log(logger):
    def logFunction(func):
        def execFunction(*args, **kwargs):
            logger.info("Executing %s", func.__name__)
            try:
                exec_res = func(*args, **kwargs)
                logger.info("Completed executing %s", func.__name__)
                return exec_res
            except:
                logger.error("Exception thrown for " + func.__name__, exc_info=1)
        return execFunction
    return logFunction

# Check if the given date is a workday
# Input: date is either datetime.datetime or datetime.date type
def isworkday(date):
    if date.isoweekday() < 6:
        return True
    return False


# Convert UTC to local time
# datetime: a datetime object
def utc2local(time_utc):
    if time_utc.tzinfo is None:
        time_utc = pytz.timezone("UTC").localize(time_utc)
    return time_utc.astimezone(pytz.timezone(tz_local))


# Convert local time to UTC
# datetime: a datetime object
def local2utc(time_local):
    if time_local.tzinfo is None:
        time_local = pytz.timezone(tz_local).localize(time_local)
    return time_local.astimezone(pytz.timezone("UTC"))


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
    mongo_collection = mongo_client[mongodb][mongocol]
    insert_res = mongo_collection.insert_many(data)
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
    # print("finshed sending email")


# Create logger with specified handlers
# name: name of the logger
# level: level of the parent logger
# handlers: a list of handlers attached to the logger [handler1_specs, hander2_specs, ...]
#           handler_spec: a dict contains all specs of the handler
#                       {type: type_of_handler,
#                       formatter: format_of_handler
#                       level: level,
#                       options: dict_of_options_of_the_handler}
def getLogger(name, level=logging.INFO, handlers=None):
    logger = logging.getLogger(name=name)
    logger.setLevel(level)
    for idx in range(len(handlers)):
        handler = handlers[idx]
        handler_format, handler_level, handler_options = log_format, logging.INFO, {}
        if "formatter" in handler.keys():
            handler_format = handler["formmater"]
        if "level" in handler.keys():
            handler_level = handler["level"]
        if "options" in handler.keys():
            handler_options = handler["options"]
        addLoggerHandler(logger, handler["type"], handler_level, handler_format, handler_options)
    return logger    


# Attach handler to the logger
def addLoggerHandler(logger, handler_type, level, formatter, options={}):
    if handler_type == "stream":
        stream = sys.stdout
        if "stream" in options.keys():
            stream = options["stream"]
        handler = logging.StreamHandler(stream=stream)
    elif handler_type == "TimeRotatingFileHandler":
        handler = logging.handlers.TimedRotatingFileHandler(
            filename=options["filename"], backupCount=180, encoding="utf8", delay=True)
        handler.suffix = options["suffix"]
    else:
        handler = None
    if handler is not None:
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(fmt=formatter, datefmt=log_date_format))
        logger.addHandler(handler)


def Logger(name=None, level=logging.INFO):
    handlers = [
        {
            "type": "TimeRotatingFileHandler",
            "level": logging.INFO,
            "options": {
                "filename": log_file_info,
                "suffix": handler_rollover_suffix
            }
        },
        {
            "type": "TimeRotatingFileHandler",
            "level": logging.ERROR,
            "options": {
                "filename": log_file_error,
                "suffix": handler_rollover_suffix
            }
        }
    ]
    logger = getLogger(name=name, level=level, handlers=handlers)
    return logger



# Check if a file exists
def checkFileExists(file_path):
    file = Path(file_path)
    if file.is_file():
        return True
    return False

# Check if a given data is type of list or tuple
def isListOrTuple(data):
    if type(data) in (type(list()), type(tuple())):
        return True
    return False

# Create Index for Mongo collection
# Inputs: db: MongoDB database
#         col: MongoDB collection
#         keys: a dict in the format {key: sort_order}, if the dict contains more than one key:val pair, a composite index will be created
def createMongoIdx(db, col, keys):
    mongo_client = MongoClient(host=mongo_host, port=mongo_port)
    mongo_collection = mongo_client[db][col]
    index_keys = []
    for key, val in keys.items():
        index_keys.append((key, val))
    insert_res = mongo_collection.create_index(index_keys)
    mongo_client.close()
    return insert_res


# Find Mongo doc given the conditions
# db: database name
# col: collection name
# conditions: coniditions for filtering the docs in the collection
# fields: type of tuple or list, contains the field(s) to return 
def findMongoDoc(db, col, conditions, fields=None):
    collection = MongoClient(host=mongo_host, port=mongo_port)[db][col]
    if fields is not None and isListOrTuple(fields):
        fields_tmp = {}
        for field in fields:
            fields_tmp[field] = 1
        res = collection.find(conditions, fields_tmp)
    else:
        res = collection.find(conditions)
    return res


# Find one Mongo doc given the conditions
# db: database name
# col: collection name
# conditions: coniditions for filtering the docs in the collection
# fields: type of tuple or list, contains the field(s) to return 
def findOneMongoDoc(db, col, conditions, fields=None):
    collection = MongoClient(host=mongo_host, port=mongo_port)[db][col]
    if fields is not None and isListOrTuple(fields):
        fields_tmp = {}
        for field in fields:
            fields_tmp[field] = 1
        res = collection.find_one(conditions, fields_tmp)
    else:
        res = collection.find_one(conditions)
    return res


# Find distinct values of a field in the Mongo Doc
# db: name of the Mongo database
# col: collection name
# field: the field whose distinct values to be found
# conditions: conditions used to filter the docs
def findDistinctMongoDoc(db, col, field, conditions={}):
    res = findMongoDoc(db, col, conditions).distinct(field)
    return res

        
class MongodDb():
    def __init__(self, host=mongo_host, port=mongo_port, db=None, col=None):
        self.client = MongoClient(host=host, port=port)
        if db is not None and col is not None:
            self.collection = self.client[db][col]
    
    def setCollection(self, db, col):
        self.collection = self.client[db][col]

    def delete(self, criteria):
        self.collection.delete_many(criteria)

    # Create Index for Mongo collection
    # keys: a dict in the format {key: sort_order}, if the dict contains more than one key:val pair, a composite index will be created
    def createIdx(self, keys):
        index_keys = []
        for key, val in keys.items():
            index_keys.append((key, val))
        res = self.collection.create_index(index_keys)
        return res

    # output data to mongoDB     
    def insert(self, data):
        res = self.collection.insert_many(data)
        return res
    

    # Find Mongo doc given the conditions
    # conditions: coniditions for filtering the docs in the collection
    # fields: type of tuple or list, contains the field(s) to return 
    def find(self, conditions, fields=None):
        if fields is not None and isListOrTuple(fields):
            fields_tmp = {}
            for field in fields:
                fields_tmp[field] = 1
            res = self.collection.find(conditions, fields_tmp)
        else:
            res = self.collection.find(conditions)
        return res

    # Find one Mongo doc given the conditions
    # conditions: coniditions for filtering the docs in the collection
    # fields: type of tuple or list, contains the field(s) to return 
    def findOne(self, conditions, fields=None):
        if fields is not None and isListOrTuple(fields):
            fields_tmp = {}
            for field in fields:
                fields_tmp[field] = 1
            res = self.collection.find_one(conditions, fields_tmp)
        else:
            res = self.collection.find_one(conditions)
        return res

    # Find distinct values of a field in the Mongo Doc
    # field: the field whose distinct values to be found
    # conditions: conditions used to filter the docs
    def findDistinct(self, field, conditions={}):
        res = self.find(conditions).distinct(field)
        return res
    
    def close(self):
        self.client.close()
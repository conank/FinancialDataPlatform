from pymongo import MongoClient
from settings import *
import numpy as np
import datetime
import logging
from logging import handlers
from pathlib import Path
import sys
import pytz
import time
import subprocess
import os
import MySQLdb

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
        execFunction.__name__ = func.__name__
        execFunction.__doc__ = func.__name__
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


def execShellCmd(command):
    res = subprocess.run(command, stdout=subprocess.PIPE)
    return res

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


class MongoDb:
    def __init__(self, host=mongo_host, port=mongo_port, db=None, col=None):
        self.client = MongoClient(host=host, port=port)
        if db is not None and col is not None:
            self.collection = self.client[db][col]
    
    def setCollection(self, db, col):
        self.collection = self.client[db][col]

    def delete(self, criteria):
        return self.collection.delete_many(criteria)

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
        if type(data) == type(dict()):
            res = self.collection.insert_one(data)
        else:
            res = self.collection.insert_many(data)
        return res
    

    # Find Mongo doc given the conditions
    # conditions: coniditions for filtering the docs in the collection
    # fields: type of tuple or list, contains the field(s) to return
    # return_list: if True, the returned result will be converted to a list, otherwise mongo cursor
    def find(self, conditions={}, fields=None, return_list=True):
        if fields is not None and isListOrTuple(fields):
            fields_tmp = {}
            for field in fields:
                fields_tmp[field] = 1
            res = self.collection.find(conditions, fields_tmp)
        else:
            res = self.collection.find(conditions)
        if return_list:
            return list(res)
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
        res = self.find(conditions, return_list=False).distinct(field)
        return res
    
    def close(self):
        self.client.close()

    def databaseExists(self, db):
        db_names = self.client.list_database_names()
        if db in db_names:
            return True
        return False

    # Check if the collection exists
    def collectionExists(self, db, col):
        if not self.databaseExists(db):
            return False
        collection_names = self.client[db].collection_names()
        if col in collection_names:
            return True
        return False

    def findOneAndReplace(self, criteria, replacement):
        return self.collection.find_one_and_replace(criteria, replacement)

# JobTracker automatically track the progress of a job by iterating through the keys used by the job to identify different subroutines
# JobTracker use a database named "job_tracker" in MongoDB and store the key values of the job in a collection named after the job
# The job data is stored as {key_name: key_name, key_val: [keys], status: job_status, timeCreated: timestamp}
# The job function should at least accept inputs: key_name, key_val, options
class JobTracker:
    return_code = {"status": 0, "msg": ""}
    # Initiate the job tracker
    # job: a function that runs the job
    # key: the key used to track the progress of the job {"key": key_name, "val": key_val}
    def __init__(self, job, job_info=None):
        self.job = job
        self.mongodb = MongoDb(db=jobtracker_mongodb, col=jobtracker_mongocol)
        if job_info is not None:
            self.setJobInfo(job_info)

    def setJobInfo(self, job_info, status=JobStatus.Initiated.value, job=None):
        # Allow users to set a new job instead of only job info
        if job is not None:
            self.job = job
        if self.jobExists():
            self.mongodb.delete({"job": self.job.__name__})
        self.mongodb.insert(self.constructJobRecord(job_info, status))

    def constructJobRecord(self, job_info, status=JobStatus.Initiated.value, job=None):
        if job is None:
            job = self.job
        record = {"job": job.__name__, "key_name": job_info["key"], "key_val": job_info["val"],
                  "status": status, "timestamp": datetime.datetime.utcnow(), "options": None}
        if "options" in job_info.keys():
            record["options"] = job_info["options"]
        return record

    def retrieveJobRecord(self, job=None):
        if job is None:
            job = self.job
        # Assume only one job record
        res =  self.mongodb.findOne({"job": job.__name__})
        return res

    def deleteJobRecord(self, job=None):
        if job is None:
            job = self.job
        return self.mongodb.delete({"job": job.__name__})

    # Check if a job already exists
    def jobExists(self, job=None):
        if job is None:
            job = self.job
        if not self.mongodb.collectionExists(db=jobtracker_mongodb, col=jobtracker_mongocol):
            return False
        if len(self.mongodb.find({"job": job.__name__})) >= 1:
            return True
        return False

    # options: input parameters to the job fucntion
    def start(self, options=None):
        # check if the job already in progress or has finished
        if not self.jobExists(self.job):
            self.return_code["status"] = JobStatus.JobNotInitialized.value
            self.return_code["msg"] = JobStatus.Msg.value[JobStatus.JobNotInitialized.value]
            return self.return_code
        job_record = self.retrieveJobRecord()
        if options is not None:
            job_record["options"] = options

        # If job already finished, return
        if job_record["status"] == JobStatus.Finished.value:
            self.return_code["status"] = JobStatus.AlreadyFinished.value
            self.return_code["msg"] = JobStatus.Msg.value[JobStatus.AlreadyFinished.value]
            return self.return_code

        interval = 1 # Interval of job execution
        if "interval" in job_record["options"].keys() and job_record["options"]["interval"] is not None:
            interval = job_record["options"]["interval"]

        # Loop through all values of key
        while len(job_record["key_val"]) > 0:
            key_val = job_record["key_val"].pop()
            self.job(key_name=job_record["key_name"], key_val=key_val, options=job_record["options"])
            job_record["timestamp"] = datetime.datetime.utcnow()
            self.mongodb.findOneAndReplace({}, job_record)
            time.sleep(interval)

        # Mark the job as finished
        job_record["status"] = JobStatus.Finished.value
        job_record["timestamp"] = datetime.datetime.utcnow()
        self.mongodb.findOneAndReplace({}, job_record)

        # Return results
        self.return_code["status"] = JobStatus.Finished.value
        self.return_code["msg"] = JobStatus.Msg.value[JobStatus.Finished.value]
        return self.return_code



# Restore the volume to the target container which is already up and running with a volume
# source_folder: the folder that contains backup file
# source_file: the name of the backup file
# target: the container the volume to be restored into
def restoreVolume(source_folder, source_file, target):
    if "\\" in source_folder:
        source_folder += "\\" + "backup"
    else:
        source_folder += "/backup"
    cmd = ["docker", "run", "--rm", "--volumes-from", target,
           "-v", source_folder + ":/backup", "ubuntu",
           "bash", "-c", "cd " + mongo_data_path + " && tar xvf /backup/" + source_file + " --strip 1"]
    print(" ".join(cmd))
    res = execShellCmd(cmd)
    return res


# Transform dataframe returned by tushare into list of data
def transformDfToDict(df):
    data = []
    for row in range(df.shape[0]):
        stock = {}
        for col in list(df.columns):
            colDtype = df.dtypes[col]
            val = df[col][row]
            if colDtype == np.int64 or colDtype == np.int32:
                val = int(val)
            stock[col] = val
        data.append(stock)
    return data


from enum import Enum

# General settings
notify_email_addr = "shunrongshen637@live.com"
global_logger_name = "global_logger"

# MongoDB server settings
mongo_host = "localhost"
mongo_port = 27017
mongo_data_path = "/data/db"
mongo_docker_name = "mongodb"

# Jobs settings
daily_price_mongodb = "price_data"
daily_price_mongocol = "daily"
stock_class_mongodb = "stock_classification"
stock_class_mongocol_industry = "industry"
stock_class_mongocol_concept = "concept"
stock_class_mongocol_area = "area"

# Email settings
smtp_host = "smtp-mail.outlook.com"
smtp_username = "shunrongshen637.service@outlook.com"
smtp_passwd = "pwd4_Outlook"

# Logger settings
log_format = "%(asctime)s [%(levelname)s]: %(message)s"
log_date_format = "%Y-%m-%d %I:%M:%S%p"
log_folder = "logs"
log_file_info= log_folder + "/" + "info.log"
log_file_error = log_folder + "/" + "errors.log"
handler_rollover_suffix = "%Y-%m-%d"

# Timezone configuration
tz_local = "Hongkong"

# Job tracker configuration
jobtracker_mongodb = "job_tracker"
jobtracker_mongocol = "jobs"
class JobStatus(Enum):
    Initiated = 0
    Inprogress = 1
    Finished = 2
    AlreadyFinished = 3
    JobNotInitialized = 4
    Msg = {0: "Job Initiated", 1: "Job Inprogress", 2: "Job Finished", 3: "Job Already finished", 4: "Job Not Initialized"}

# Backup configuration
backup_default_file_prefix = "financial_data"

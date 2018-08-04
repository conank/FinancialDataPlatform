# General settings
notify_email_addr = "shunrongshen637@live.com"
global_logger_name = "global_logger"

# MongoDB server settings
mongo_host = "localhost"
mongo_port = 27017

# Daily price settings
daily_price_mongodb = "price_data"
daily_price_mongocol = "daily"

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
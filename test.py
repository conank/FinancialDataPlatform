from settings import *
from utils import *
from jobs import *

# Test email utils
# send_email("shunrongshen637@live.com", "Test", "Test2")

# Test logger
# import logging
# logger = Logger(global_logger_name)
# logger.info("Info")
# logger.error("Error")
# for handler in logger.handlers:
#     handler.doRollover()

# Test log wrapper
logger = Logger(global_logger_name)
@log(logging.getLogger(global_logger_name))
def div():
    return 1/0
@log(logging.getLogger(global_logger_name))
def div2():
    return 4/2
div()
div2()





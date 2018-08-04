from utils import *
from jobs_offline import *
import logging


# Initiate Logger
logger = getLogger(name=__name__, handlers=[{"type": "stream"}])

# Insert all the historical price data #
# Get all stock codes
initHistDailyPrice(start="2000-01-01")
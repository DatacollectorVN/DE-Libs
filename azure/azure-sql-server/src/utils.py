import loguru  
import pandas as pd

def get_custom_loggers():
    loguru.logger.level('BUG', no = 38, color = '<red>')
    return loguru.logger
logger = get_custom_loggers()
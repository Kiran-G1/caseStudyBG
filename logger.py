from datetime import datetime
import logging
from logging.handlers import WatchedFileHandler


class Logger:
    def __init__(self, module_name, level='info'):
        level = logging.DEBUG if level == 'debug' else logging.INFO

        current_time = datetime.strftime(datetime.now(), "%Y%m%d_%H%s")
        log_file_path = './logs/{}_{}.log'.format(module_name, current_time)
        self.logger = logging.getLogger(module_name)
        self.logger.setLevel(level)

        fh = WatchedFileHandler(log_file_path)
        fh.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s > %(funcName)s (%(lineno)d): %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

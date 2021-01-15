import logging
import rollbar


class LoggerEx:
    INFO = 1
    WARNING = 11
    ERROR = 21
    CRITICAL = 31

    def __init__(self, log_level=WARNING, rollbar_secret=None, rollbar_level=ERROR):
        self.rollbar_level = rollbar_level
        self.rollbar_enabled = False

        if rollbar_secret is not None:
            rollbar.init(rollbar_secret, 'production')
            self.rollbar_enabled = True

        if log_level <= self.INFO:
            log_level = logging.INFO
        elif log_level <= self.WARNING:
            log_level = logging.WARNING
        elif log_level <= self.ERROR:
            log_level = logging.ERROR
        elif log_level <= self.CRITICAL:
            log_level = logging.CRITICAL
        else:
            log_level = logging.WARNING

        logging.basicConfig(level=log_level)
        self.logger = logging.getLogger()
        self.logger.setLevel(log_level)

    def info(self, message):
        if self.rollbar_enabled and self.rollbar_level <= self.INFO:
            rollbar.report_message(message, 'info')
        self.logger.info(message)

    def warning(self, message):
        if self.rollbar_enabled and self.rollbar_level <= self.WARNING:
            rollbar.report_message(message, 'warning')
        self.logger.warning(message)

    def error(self, message):
        if self.rollbar_enabled and self.rollbar_level <= self.ERROR:
            rollbar.report_message(message, 'error')
        self.logger.error(message)

    def critical(self, message):
        if self.rollbar_enabled and self.rollbar_level <= self.CRITICAL:
            rollbar.report_message(message, 'critical')
        self.logger.critical(message)

    def report_exception(self):
        if self.rollbar_enabled:
            rollbar.report_exc_info()


logger = LoggerEx(LoggerEx.INFO)

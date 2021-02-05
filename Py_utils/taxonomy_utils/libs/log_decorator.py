import logging


def log_decorate(func, formattor:str='%(asctime)s:%(process)2s %(threadName)10s %(name)s: %(message)s\n', **kwargs):
    ROOT_LOGGER = logging.getLogger()
    exiting_formattor = ROOT_LOGGER.handlers[0].formatter
    ROOT_LOGGER.handlers[0].setFormatter(logging.Formatter(formattor))
    ret = func(**kwargs)
    ROOT_LOGGER.handlers[0].setFormatter(exiting_formattor)
    return ret
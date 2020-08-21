import logging

logger = logging.getLogger(__package__)
fmt = logging.Formatter('%(asctime)s,%(process)d,%(name)s,%(levelname)s,%(filename)s:%(lineno)d,%(message)s')
sh = logging.StreamHandler()
sh.setFormatter(fmt)
sh.setLevel(logging.DEBUG)
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

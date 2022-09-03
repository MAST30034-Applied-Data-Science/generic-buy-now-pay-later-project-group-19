import logging

logging.basicConfig(filename="./log/log.txt",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

logging.info("Start Logging")

logger = logging.getLogger('logger')


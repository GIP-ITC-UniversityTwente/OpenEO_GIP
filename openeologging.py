import logging
import threading


lockLogger = threading.Lock()

def logMessage(level, message, user='system'):
      lockLogger.acquire()
      logger = logging.getLogger('openeo')
      logger.log(level, '[ ' + user + ' ] '  + message)
      lockLogger.release()

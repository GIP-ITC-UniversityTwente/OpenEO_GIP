import logging
import threading
import os


lockLogger = threading.RLock()

def logMessage(level, message, user='system'):
      #lockLogger.acquire()
      logger = logging.getLogger('openeo')
      logger.log(level, '[ ' + user + ' ] '  + message)
      #lockLogger.release()

def notRunnableError(name, job_name):
     message = "operation not runnable:" + name + "job id:" + str(job_name)
     logMessage(logging.ERROR, message)
     return message

def makeFolder(path, user='system'):
    try:
        if ( not os.path.exists(path)):
            logMessage(logging.INFO, 'made folder:'+ path)
            os.makedirs(path)
    except Exception as ex:
        raise Exception('server error. could not make:' + path)         

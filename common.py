from flask import make_response, jsonify, Response
from werkzeug.wsgi import FileWrapper
import json
from pathlib import Path
import os
import pickle
from multiprocessing import Lock
import logging
import mimetypes
from constants.constants import *
from io import BytesIO
from zipfile import ZipFile
import os
from datetime import datetime

lock = Lock()

current_dir = os.path.dirname(os.path.abspath(__file__))
configPath = os.path.join(current_dir,'config/config.json' )
openeoip_configfile = open(configPath)
openeoip_config = json.load(openeoip_configfile)

codesfile = open('./config/default_error_codes.json')
default_errrors = json.load(codesfile) 

raster_data_sets = None

def getRasterDataSets():
    home = Path.home()
    loc = openeoip_config['data_locations']['system_files']
    sytemFolder = os.path.join(home, loc['location'])        
    propertiesFolder = os.path.join(home, sytemFolder)
    if ( os.path.exists(propertiesFolder)):
        propertiesPath = os.path.join(propertiesFolder, 'id2filename.table')
        if ( os.path.exists(propertiesPath)):
            lock.acquire()
            with open(propertiesPath, 'rb') as f:
                data = f.read()
            f.close()
            lock.release()    
            raster_data_sets =  pickle.loads(data) 
    return raster_data_sets

def saveIdDatabase(idDatabse):
        home = Path.home()
        loc = openeoip_config['data_locations']['system_files']
        sytemFolder = os.path.join(home, loc['location'])
        propertiesFolder = os.path.join(home, sytemFolder)
        if ( not os.path.exists(propertiesFolder)):
            os.makedirs(propertiesFolder)
        propsPath = os.path.join(propertiesFolder, 'id2filename.table')
        propsFile = open(propsPath, 'wb')
        pickle.dump(idDatabse, propsFile)
        propsFile.close() 

def logMessage(level, message, user='default'):
      logger = logging.getLogger('openeo')
      logger.log(level, message)

def notRunnableError(name, job_name):
     message = "operation not runnable:" + name + "job id:" + str(job_name)
     logMessage(logging.ERROR, message)
     return message

def makeFolder(path, user='default'):
    try:
        if ( not os.path.exists(path)):
            logMessage(logging.INFO, 'could not open:'+ path)
            os.makedirs(path)
    except Exception as ex:
        raise Exception('server error. could not make:' + path)         

def inspectFileType(filename):
    type, encoding = mimetypes.guess_type(filename)
    role ='data'
    if type == 'image/tiff':
        type = type + ';application=geotiff'
    if type == 'application/json' or type == 'application/xml':
         role = 'metadata'
    return type, role  


def makeResponse(outputInfo):
    if outputInfo["status"] == STATUSFINISHED:
        if outputInfo["datatype"] == DTRASTER or outputInfo["datatype"] == DTRASTERLIST :
            if len(outputInfo["value"]) ==1:
                filename = outputInfo["value"][0]
                mimet = mimetypes.guess_type(filename)
                with open(filename, 'rb') as file:
                    binary_data = file.read()
                    response = Response(binary_data,
                                    mimetype=mimet[0],
                                    direct_passthrough=True)
            else:   
                stream = BytesIO()
                now = datetime.now()
                date_string = now.strftime("%Y%m%d%H%M%S")
                date_int = int(date_string)
                with ZipFile(stream, 'a') as zf:                                                         
                    for fn in outputInfo["value"]:
                        zf.write(fn, os.path.basename(str(date_int) + ".zip"))
                    stream.seek(0)
                    w = FileWrapper(stream)
                    response = Response(w,
                                    mimetype="application/x-zip",
                                    direct_passthrough=True)

                
        elif outputInfo["datatype"] != DTRASTER:
            response = Response(str(outputInfo["value"]), mimetype = "string", direct_passthrough=True)
            response.headers['Content-Type'] = 'string'

        return response
    return None            

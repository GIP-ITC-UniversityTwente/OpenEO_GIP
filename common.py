from flask import make_response, jsonify, Response
from werkzeug.wsgi import FileWrapper
import json
from pathlib import Path
from multiprocessing import Lock
import logging
import mimetypes
from constants.constants import *
from io import BytesIO
from zipfile import ZipFile
import os, shutil
from datetime import datetime
from dateutil import parser
# import tests.addTestRasters as tr
from processmanager import lockLogger
import ilwis
import glob

ilwobj_created_ids = {}


possible_time_formats = [
    "%Y-%m-%d %H:%M:%S",  # Format 1: YYYY-MM-DD HH:MM:SS
    "%m/%d/%Y %I:%M %p",   # Format 2: MM/DD/YYYY HH:MM AM/PM
    "%Y-%m-%d"             # Format 3: YYYY-MM-DD
    # Add more formats as needed
]
lock = Lock()

current_dir = os.path.dirname(os.path.abspath(__file__))
configPath = os.path.join(current_dir,'config/config.json' )
openeoip_configfile = open(configPath)
openeoip_config = json.load(openeoip_configfile)

codesfile = open('./config/default_error_codes.json')
default_errrors = json.load(codesfile) 

raster_data_sets = None
process_user = 'system'
testRaster_openeo1  = None



# registers a data set in the system
def saveIdDatabase(idDatabse):
        home = Path.home()
        # retrieve the root syste, location from the config
        loc = openeoip_config['data_locations']['system_files']
        sytemFolder = os.path.join(home, loc['location'])
        propertiesFolder = os.path.join(home, sytemFolder)
        # if the property folder doesnt exist create it
        if ( not os.path.exists(propertiesFolder)):
            os.makedirs(propertiesFolder)
        propsPath = os.path.join(propertiesFolder, 'id2filename.table')
        propsFile = open(propsPath, 'w')
        s = json.dumps(idDatabse, default=str) 
        propsFile.write(s) 
        propsFile.close() 

# sets a message in the logger
def logMessage(level, message, user='system'):
      lockLogger.acquire()
      logger = logging.getLogger('openeo')
      logger.log(level, '[ ' + user + ' ] '  + message)
      lockLogger.release()

def notRunnableError(name, job_name):
     message = "operation not runnable:" + name + "job id:" + str(job_name)
     logMessage(logging.ERROR, message)
     return message

def makeFolder(path, user='system'):
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

def errorJson(errorStringCode, id, message):
        if errorStringCode == CUSTOMERROR:
            return {"id" : id, "code" : 400, "message" : message }
        else:
            if errorStringCode in default_errrors:
                err = default_errrors[errorStringCode]
                predefCode = err['http']
                message = err['message']                   
                return {"id" : id, "code" : predefCode, "message" : message }
                
        return {"id" : id, "code" : 400, "message" : message }

def makeResponse(outputInfo, context = None):
    if outputInfo == None:
         return  {"id" : -1, "code" : 400, "message" : 'no output information' }
    
    if outputInfo["status"] == STATUSFINISHED:
        if outputInfo["datatype"] == DTRASTER or outputInfo["datatype"] == DTRASTERLIST :
            if len(outputInfo["value"]) ==1:
                vs = outputInfo["value"]
                filename = vs[0]
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
            if context != None:
                if isinstance(context, dict):
                    if 'removedata' in context:
                        job_id = context['removedata']
                        filePath = openeoip_config['data_locations']['root_user_data_location']
                        filePath = filePath['location'] + '/' + str(job_id)  
                        if os.path.isdir(filePath):
                            shutil.rmtree(filePath)    
                
        elif outputInfo["datatype"] != DTRASTER:
            response = Response(str(outputInfo["value"]), mimetype = "string", direct_passthrough=True)
            response.headers['Content-Type'] = 'string'

        return response
    return None 


def string2datetime(inpTimeString):
    for fmt in possible_time_formats:
        try:
            parsed_datetime = datetime.strptime(inpTimeString, fmt)
            return parsed_datetime
        except ValueError:
            continue  # If parsing fails, try the next format
    return None


def temporalOverlap(l1, l2):
    d00 = parser.parse(l1[0])
    d01 = parser.parse(l1[1])
    d10 = parser.parse(l2[0])
    d11 = parser.parse(l2[1])
    return not (d00 < d10 and d00 < d11 and d01 < d10 and d01 < d11)

def registerIlwisIds(job_id, objs):
    if not isinstance(objs, list):
        objs = [objs]
    if not job_id in ilwobj_created_ids:
        ilwobj_created_ids[job_id] = []        
    for obj in objs:
 
        ilwobj_created_ids[job_id].append(obj.ilwisID())
    return ilwobj_created_ids

def getIdsForJob(job_id):
    result = []
    if job_id in ilwobj_created_ids:
        result = ilwobj_created_ids[job_id]
    return result        

def removeTempFiles(job_id):
        ids = getIdsForJob(job_id)                
        tempFiles = ilwis.contextProperty('cachelocation')
        for id in ids:
            ilwis.removeObject(id)
            mask = os.path.join(tempFiles, '*' + str(id) + '*.temp') 
            file_list = glob.glob(mask)
            for file in file_list:
                os.remove(file)
from flask_restful import Resource
from flask import make_response, jsonify, request
from constants.constants import *
from workflow.openeoprocess import OpenEOProcess
from userinfo import UserInfo
from processmanager import makeBaseResponseDict
from authentication import AuthenticatedResource
import pathlib
import logging
import common
import datetime
from constants import constants
from customexception import CustomException
import ilwis
import os
import glob

def getMimeType(filename):
    try:
        ext = pathlib.Path(filename).suffix
        if ext == '.jpg':
            return "image/jpeg"
        elif ext == '.tif' or ext == '.tiff':
            return 'image/tiff'
        elif ext == '.png':
            return "image/png" 
        return 'application/octet-stream'
    except:
        return 'application/octet-stream'
          
        
class OpenEOIPResult(AuthenticatedResource):
    def post(self):
        request_json = request.get_json()
        user = UserInfo(request)
        try:
            process = OpenEOProcess(user, request_json,0)
            
            ''' seems the client already sends a validate request befor trying to run it so this one is superflous
            errors =  process.validate()
            if len(errors) > 0:
                raise Exception(str(errors))
            '''
            common.logMessage(logging.INFO, 'started sync: ' + process.job_id , common.process_user)
            if process.processGraph != None:
                outputInfo = process.processGraph.run(process, None, None)
                common.logMessage(logging.INFO, 'ended sync: ' + process.job_id , common.process_user)
                common.removeTempFiles(process.job_id)
                return common.makeResponse(outputInfo)#, {'removedata' : process.job_id})
        except (Exception, CustomException) as ex:
            code = 'unknow error'
            if isinstance(ex, CustomException):
                code = ex.jsonErr['code']
                message = ex.jsonErr['message']
                common.logMessage(logging.ERROR, 'error: ' + message , common.process_user)
                return make_response(makeBaseResponseDict(process.job_id, 'error', code, None, message),int(code))
            return make_response(makeBaseResponseDict(-1, 'error', 404, None, str(ex)),400)

   
        

    def makeType(self, tp):
        if ( tp == DTNUMBER):
            return "number"
        if ( tp == DTRASTER):
            return "raster"
        return "unknown"

                        
                        

                


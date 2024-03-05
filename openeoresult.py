from flask_restful import Resource
from flask import make_response, jsonify, request, Response
from constants.constants import *
from workflow.openeoprocess import OpenEOProcess
from userinfo import UserInfo
from processmanager import makeBaseResponseDict
from authentication import AuthenticatedResource
import pathlib
import common

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
            errors =  process.validate()
            if len(errors) > 0:
                raise Exception(str(errors))
            
            if process.processGraph != None:
                outputInfo = process.processGraph.run(process, None, None)

                return common.makeResponse(outputInfo)
        except Exception as ex:
            return make_response(makeBaseResponseDict(-1, 'error', 404, None, str(ex)),400)
        

    def makeType(self, tp):
        if ( tp == DTNUMBER):
            return "number"
        if ( tp == DTRASTER):
            return "raster"
        return "unknown"

                        
                        

                


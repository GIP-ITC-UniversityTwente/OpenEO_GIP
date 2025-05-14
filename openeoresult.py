from flask_restful import Resource
from flask import make_response, jsonify, request
from constants.constants import *
from workflow.openeoprocess import OpenEOProcess
from userinfo import UserInfo
from processmanager import makeBaseResponseDict
from authentication import AuthenticatedResource
import common
import openeologging
from constants import constants
from customexception import CustomException


class OpenEOIPResult(AuthenticatedResource):
    def post(self):
        request_json = request.get_json()
        user = UserInfo(request)
        try:
            process = OpenEOProcess(user, request_json,0)
            
            openeologging.logMessage(logging.INFO, 'started sync: ' + process.job_id , common.process_user)
            if process.processGraph != None:
                outputInfo = process.processGraph.run(process, None, None)
                openeologging.logMessage(logging.INFO, 'ended sync: ' + process.job_id , common.process_user)
                common.removeTempFiles(process.job_id)
                return common.makeResponse(outputInfo)#, {'removedata' : process.job_id})
        except (Exception, CustomException) as ex:
            code = 'unknow error'
            if isinstance(ex, CustomException):
                code = ex.jsonErr['code']
                message = ex.jsonErr['message']
                openeologging.logMessage(logging.ERROR, 'error: ' + message , common.process_user)
                return make_response(makeBaseResponseDict(process.job_id, 'error', code, None, message),int(code))
            return make_response(makeBaseResponseDict(-1, 'error', 404, None, str(ex)),400)


    def makeType(self, tp):
        if ( tp == DTNUMBER):
            return "number"
        if ( tp == DTRASTER):
            return "raster"
        return "unknown"

                        
                        

                


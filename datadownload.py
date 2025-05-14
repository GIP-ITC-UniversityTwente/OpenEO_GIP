from flask_restful import Resource
from flask import make_response, jsonify, request, Response
from itsdangerous import URLSafeTimedSerializer
from processmanager import globalProcessManager
from datetime import datetime
from globals import globalsSingleton
import common 
import openeologging
import os
import mimetypes
import logging
from userinfo import UserInfo

class OpenEODataDownload(Resource):
    def get(self, token):
        secret = globalsSingleton.signed_url_secret 
        user = UserInfo(request)
        s = URLSafeTimedSerializer(secret)
        parts = token.split('___')
        if len(parts) == 2:
            folder_token = parts[0]
            resultName = parts[1]
            folder = ''
            openeologging.logMessage(logging.INFO,'prepare downloading ' + resultName,user.username) 
            try:
                folder = s.loads(folder_token)['user_id']
                now = datetime.now()
                if globalProcessManager.outputs[folder].eoprocess.user.username != user.username:
                    openeologging.logMessage(logging.ERROR,'credentials invalid for '+ resultName,user.username) 
                    err = globalsSingleton.errorJson('CredentialsInvalid', user.username,'')
                    return make_response(jsonify(err),err.code) 
                
                then = globalProcessManager.outputs[folder].availableStart
                delta = now - then
                n = delta.total_seconds()
                if n > 30*60:
                    err = globalsSingleton.errorJson('ResultLinkExpired', folder,'')
                    return make_response(jsonify(err),err.code)
                
                path = common.openeoip_config['data_locations']['root_user_data_location']
                path = path['location']
                fullpath = os.path.join(path, folder) 
                outFile = os.path.join(fullpath, resultName) 
                if os.path.exists(outFile):
                    mimet = mimetypes.guess_type(outFile)
                    with open(outFile, 'rb') as file:
                        binary_data = file.read()
                        response = Response(binary_data,
                                    mimetype=mimet[0],
                                    direct_passthrough=True)
                        return response
            except Exception as ex:
                openeologging.logMessage(logging.ERROR,'failed download result with error'+ str(ex),common.process_user) 
                err = globalsSingleton.errorJson('FileNotFound', 'system','')
                return make_response(jsonify(err),err.code) 
            return make_response(jsonify({ "errors": 'test'}), 200)
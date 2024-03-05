from flask_restful import Resource
from flask import make_response, jsonify, request, Response
from werkzeug.wsgi import FileWrapper
from itsdangerous import URLSafeTimedSerializer
from processmanager import globalProcessManager
from datetime import datetime, timedelta
from globals import globalsSingleton
import common 
import os
import json
import mimetypes
from io import BytesIO
from io import BytesIO
from zipfile import ZipFile

class OpenEODataDownload(Resource):
    def get(self, token):
        ss = request.base_url
        secret = globalsSingleton.signed_url_secret 
        s = URLSafeTimedSerializer(secret)
        idx = token.rfind('___')
        if idx != -1:
            folder_token = token[:idx]
            folder = ''
            try:
                folder = s.loads(folder_token)['user_id']
                now = datetime.now()
                then = globalProcessManager.outputs[folder].availableStart
                delta = now - then
                n = delta.total_seconds()
                if n > 30*60:
                    err = globalsSingleton.errorJson('ResultLinkExpired', folder,'')
                    return make_response(jsonify(err),err.code)
                
                path = common.openeoip_config['data_locations']['root_user_data_location']
                path = path['location']
                fullpath = os.path.join(path, folder)  
                outFiles = [f for f in os.listdir(fullpath) if f != "jobmetadata.json"]                                 
                if len(outFiles) == 1:
                    outFile = os.path.join(fullpath, outFiles[0])
                    mimet = mimetypes.guess_type(outFile)
                    with open(outFile, 'rb') as file:
                        binary_data = file.read()
                        response = Response(binary_data,
                                    mimetype=mimet[0],
                                    direct_passthrough=True)
                        return response
                else:
                    stream = BytesIO()
                    now = datetime.now()
                    date_string = now.strftime("%Y%m%d%H%M%S")
                    date_int = int(date_string)                    
                    with ZipFile(stream, 'a') as zf:                                                         
                        for fn in outFiles:
                            outFile = os.path.join(fullpath, fn)
                            zf.write(outFile, os.path.basename(str(date_int) + ".zip"))
                        stream.seek(0)
                        w = FileWrapper(stream) 
                        
                        response = Response(w,
                                        mimetype="application/x-zip",
                                        direct_passthrough=True)
                        return response

            except:
                return None
            return make_response(jsonify({ "errors": 'test'}), 200)
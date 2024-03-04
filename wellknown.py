from flask_restful import Resource
from flask import make_response, jsonify, request
import common

class WellKnown(Resource):

    def __init__(self):
        Resource.__init__(self) 
              
    def get(self):        
        version_list = list()
        version_list.append({"url": common.openeoip_config['openeo_gip_root'],
                             "api_version": "1.2.0",
                             "production": False})

        resp = dict()
        resp['versions'] = version_list

        return make_response(jsonify(resp), 200)

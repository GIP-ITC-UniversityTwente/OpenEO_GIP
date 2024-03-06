from flask import make_response, jsonify, request, url_for
from flask_restful import Resource
from workflow.openeoprocess import OpenEOProcess
from processmanager import globalProcessManager, makeBaseResponseDict
from userinfo import UserInfo
from constants.constants import *
from globals import globalsSingleton
from constants import constants
from authentication import AuthenticatedResource
import common
import os
import json
from itsdangerous import URLSafeTimedSerializer
from datetime import datetime

class OpenEOIPJobs(AuthenticatedResource):
    def processPostJobId(self, user, request_json):
        try:
            process = OpenEOProcess(user, request_json, 0)
            globalProcessManager.addProcess(process)
            url = request.base_url + "/" + process.job_id
            response =  make_response(jsonify(process.job_id),201)
            response.headers['OpenEO-Identifier'] = process.job_id
            response.headers['Location'] = url
            return response
        except Exception as ex:
            err = globalsSingleton.errorJson(constants.CUSTOMERROR, 400, str(ex))
            return make_response(jsonify(err), err.code)   

    def processGetJobs(self, user): 
        try:        
            jobs = globalProcessManager.allJobsMetadata4User(user, None,request.base_url)
            return  make_response(jsonify({'jobs' : jobs}),200)
        except Exception as ex:
            err = globalsSingleton.errorJson(constants.CUSTOMERROR, 400, str(ex))
            return make_response(jsonify(err), err.code) 
                      

    def post(self):
            request_json = request.get_json()
            user = UserInfo(request)
            return self.processPostJobId(user, request_json)
    def get(self):
            user = UserInfo(request)
            return self.processGetJobs(user)
          
class OpenEOJobResults(AuthenticatedResource):
   def returnJobResultUrls(self, job_id, user, host):
        path = common.openeoip_config['data_locations']['root_user_data_location']
        rootJobdataPath = os.path.join(path['location'],str(job_id))
        path = os.path.join(rootJobdataPath, "jobmetadata.json") 
        if os.path.exists(path):
            with open(path, "r") as fp:
                    eoprocess = json.load(fp)           
            ##eoprocess = globalProcessManager.allJobsMetadata4User(user, job_id, request.base_url)
            result = {}
            result['stac_version'] = globalsSingleton.openeoip_config['stac_version']
            result['id'] = job_id
            result['type'] = 'Feature'
            result["stac_extensions"] = ["https://stac-extensions.github.io/eo/v1.0.0/schema.json", 
                                            "https://stac-extensions.github.io/view/v1.0.0/schema.json",
                                            "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
                                            "https://stac-extensions.github.io/raster/v1.1.0/schema.json"]
            if 'spatialextent' in eoprocess:
                result['bbox'] = eoprocess['spatialextent']
                arr = result['bbox']
                coords = [[arr[0], arr[1]], [arr[2], arr[1]], [arr[2], arr[3]],[arr[0], arr[3]], [arr[0], arr[1]]] 
                result['geometry'] = {'type' : 'Polygon',  'coordinate': coords}
            properties = {}
            properties['start_datetime'] = eoprocess['start_datetime']
            properties['end_datetime'] = eoprocess['end_datetime']
            properties['datetime'] = eoprocess['submitted'] 
            properties['title'] = eoprocess['title']
            properties['description'] = eoprocess['description']
            properties['openeo:status'] = 'finished'
            result['properties'] = properties
            assets = {}
            # Iterate directory
            for path in os.listdir(rootJobdataPath):
                if os.path.isfile(os.path.join(rootJobdataPath, path)):
                    if path == 'jobmetadata.json':
                        continue
                    fullpath = os.path.join(rootJobdataPath,path) 
                    type, role = common.inspectFileType(fullpath)
                    secret = globalsSingleton.signed_url_secret 
                    s = URLSafeTimedSerializer(secret)
                    token = s.dumps({'user_id': job_id })
                    url1 = 'openeodatadownload'
                    urlep = url_for(url1, token=token)
                    urldl = "http://"+ host + urlep + "___" + path
                    item = {'href' : urldl, 'type': type, 'title' : path, 'roles' : [role]}                       
                    assets[path] = item
            result['assets'] = assets 
            globalProcessManager.outputs[job_id].availableStart = datetime.now()
            return make_response(jsonify(result),200)
        else:
              err = globalsSingleton.errorJson('JobNotFinished', job_id,'')
              return make_response(jsonify(err),err.code) 



        return 
       
   def queueJob(self, job_id, user):
        try:
            message, error = globalProcessManager.queueJob(user, job_id)
            if error == "":
                return make_response(message, 202)
            else:
                err = globalsSingleton.errorJson(error, job_id, message)
                return make_response(jsonify(err),err.code)
                
        except Exception as ex:
            err = globalsSingleton.errorJson(constants.CUSTOMERROR, job_id, str(ex))
            return make_response(jsonify(err), err.code)

   def processDeleteId(self, job_id, user):
        try:
            globalProcessManager.stopJob(job_id, user)        
            res = makeBaseResponseDict(job_id,'canceled', 204,request.base_url,'job has been successfully deleted' )
            return make_response(jsonify(res),204) 
        except Exception as ex:
            return make_response(makeBaseResponseDict(-1, 'error', 404, None, str(ex)))              
       
   def post(self, job_id):
        user = UserInfo(request)
        return self.queueJob(job_id, user)  

   def get(self, job_id):
        user = UserInfo(request)
        host =  request.environ['HTTP_HOST']
        return self.returnJobResultUrls(job_id, user, host)
   def delete(self, job_id):
        user = UserInfo(request)
        return self.processDeleteId(job_id, user)
   
class OpenEOIJobByIdEstimate(AuthenticatedResource):
   def processGetEstimate(self, job_id, user):
        try:
            estimate = globalProcessManager.makeEstimate(user, job_id)
            costs = estimate[0][2]
            return make_response(jsonify(costs),200) 
        except Exception as ex:
            err = globalsSingleton.errorJson(constants.CUSTOMERROR, job_id, str(ex))
            return make_response(jsonify(err), err.code)       
       
   def get(self, job_id):
        user = UserInfo(request)
        return self.processGetEstimate(job_id, user)
 
        
class OpenEOMetadata4JobById(AuthenticatedResource):
    def processGetJobId(self, job_id, request):
        try:
            user = UserInfo(request)
            job = globalProcessManager.allJobsMetadata4User(user, job_id,request.base_url)
            return make_response(jsonify(job),200)
        except Exception as ex:
            err = globalsSingleton.errorJson(constants.CUSTOMERROR, job_id, str(ex))
            return make_response(jsonify(err), err.code)  
        
    def processPatchId(self, job_id, user, request_json):
        try:
            status = globalProcessManager.removedCreatedJob(job_id)
            if status == STATUSCREATED:
                process = OpenEOProcess(user, request_json, job_id)
                globalProcessManager.addProcess(process)
                res = makeBaseResponseDict(job_id,'updated', 204,request.base_url )
                
                return make_response(jsonify(res),204)
            if status == STATUSQUEUED:
                err = globalsSingleton.errorJson("JobLocked", job_id, str(ex))
                return make_response(jsonify(err), err.code)      
               
        except Exception as ex:
            err = globalsSingleton.errorJson(constants.CUSTOMERROR, job_id, str(ex))
            return make_response(jsonify(err), err.code)    


    def get(self, job_id):
        return self.processGetJobId(job_id, request)

      
    def patch(self, job_id):
        request_json = request.get_json()
        user = UserInfo(request)
        return self.processPatchId(job_id, user, request_json)
     

        

        

from processGraph import ProcessGraph
from globals import getOperation
from constants import constants
import multiprocessing
from datetime import datetime
import uuid
from multiprocessing import Pipe
import json
import logging
import common
import os
from openeooperation import put2Queue
import customexception


def get(key,values,  defaultValue):
    if key in values:
        return values[key]
    return defaultValue


class OpenEOParameter:
    def __init__(self, parm):
        self.schema = parm['schema']
        subt = self.schema['subtype']
        self.name = get('name', parm, '')
        self.description = get('description', parm, '')
        self.optional = get('optional', parm, False)
        self.deprecated = get('deprecated', parm, False)
        self.experimental = get('experimental', parm, False)
        self.default = get('default', parm, None)

        if subt == 'process-graph': 
            ret = parm['return']
            self.returnValue = (ret['description'], ret['schema'])
            self.parameters = [] 
            parms = parm['parameters']
            for parm in parms:
                self.parameters.append(OpenEOParameter(parm))

        if subt == 'datacube':
            dimensions = parm['dimensions']
            self.spatial_organization = []
            for dim in dimensions:
                tp = dim['type']
                if tp == 'spatial':
                    self.spatial_organization.append((tp, get('axis', dimensions, '')))
                if tp == 'geometry':
                    self.spatial_organization.append((tp, get('geometry', dimensions, '')))
                if tp in ['bands', 'temporal', 'other']:
                    self.spatial_organization.append(tp, tp)

    def toDict(self):
            parmDict = {}
            parmDict = self.schema
            parmDict['name'] = self.name
            parmDict['description'] = self.description
            parmDict['optional'] = self.optional
            parmDict['deprecated'] = self.deprecated
            parmDict['experimental'] = self.experimental
            if self.default != None:
                parmDict['default']  = self.default
            return parmDict                



class OpenEOProcess(multiprocessing.Process):
    def __init__(self, user, request_json, id):

        if not ('process' in request_json or  'process_graph' in request_json):
           raise Exception("missing \'process\' key in definition")
        old_api_version = 'process' in request_json # not all versions have a process key; some directly do process_graph        
        if old_api_version:
            self.title = get('title', request_json, '')
            self.description = get('description', request_json, '')
            self.plan = get('plan', request_json, 'none')
            self.budget = get('budget', request_json, constants.UNDEFNUMBER)
            self.log_level = get('log_level', request_json, 'all')        
            self.user = user
            processValues = request_json['process']
            ##as we are running in a seperate process the value of common.process_user is unique for this process
            common.process_user = user.username
        else:
            processValues = request_json
        self.spatialextent = []
        self.processGraph = None

        self.processGraph = ProcessGraph(get('process_graph', processValues, None), None, getOperation)
        self.submitted = str(datetime.now())
        self.status = constants.STATUSCREATED
        self.updated =  ''
        self.id = get('id', processValues, '')
        self.summary = get('summary', processValues, '')
        self.process_description = get('description', processValues, '')
        if id == 0:
            self.job_id = str(uuid.uuid4())
        else:
            self.job_id = id
        if (not hasattr(self, 'title')) :
            self.title = "job " + self.job_id 
        else:
            if self.title == '':
                self.title = "job " + self.job_id         
        self.processGraph.title = self.title                      

        self.parameters = []
        if 'parameters' in processValues:
            for parameter in processValues['parameters']:
                self.parameters.append(OpenEOParameter(parameter))
        self.categories = get('categories', processValues, [])
        self.deprecated = get('deprecated', processValues, False)
        self.experimental = get('experimental', processValues, False)


        self.exceptions = {}
        if "exceptions" in processValues:
            for ex in processValues['exceptions'].items():
                self.exceptions[ex[0]] = ex[1]        
                       
        self.returns = {}
        if 'returns' in processValues:
            returns = processValues['returns']

            self.returns['description'] = get('description', returns, '')
            
            if 'schema' in returns:
                self.returns['schema'] = returns['schema']
            else:
                raise Exception("missing \'schema\' key returns definition")
            
        self.examples = []
        if 'examples' in processValues:
            self.examples = get('examples', processValues, [])

        self.links = []
        if 'links' in processValues:
            self.links = get('links', processValues, [])   

        self.sendTo, self.fromServer = Pipe() #note: these pipes are only used for ouput to the child process

    def validate(self):
        errorsdict = []
        errors = self.processGraph.validateGraph()
        for error in errors:
            errorsdict.append({ "id" : self.job_id, "code" : "missing operation", "message" : error})
            
        return errorsdict             

    def setItem(self, key, dict):
        if hasattr(self, key):
            dict[key] = getattr(self, key)
        return dict
    
    def setItem2(self, key, dict, alt):
        if hasattr(self, alt):
            dict[key] = getattr(self, alt)
        return dict

    def estimate(self, user):
        return self.processGraph.estimate()

    def toDict(self, short=True):
        dictForm = {}
        dictForm['id'] = str(self.job_id)
        dictForm = self.setItem('title', dictForm)
        dictForm = self.setItem('description', dictForm)
        dictForm = self.setItem('deprecated', dictForm)
        dictForm = self.setItem('experimental', dictForm)
        dictForm = self.setItem('submitted', dictForm)
        dictForm = self.setItem('plan', dictForm)
        dictForm = self.setItem('budget', dictForm)
        if short == False:
            processDict = {}
            processDict = self.setItem('summary', processDict) 
            processDict = self.setItem2('id', processDict,'title') 
            processDict = self.setItem('desciption', processDict)
            parms = []
            for parm in self.parameters:
                parms.append(parm.toDict())
            if len(parms) > 0:
                processDict["parameters"] = parms

            processDict['returns'] = self.returns
            processDict['categories'] = self.categories
            if len(self.examples) > 0:
                processDict['examples'] = self.examples 
            if len(self.links) > 0:
                processDict['links'] = self.examples                 
            processDict['process_graph'] = self.processGraph.sourceGraph
            dictForm['process'] = processDict
            dictForm['log_level'] = self.log_level
            dictForm['spatialextent'] = self.spatialextent

        return dictForm  

    def cleanup(self):
        ##TODO : remove data
        a = 1

    def stop(self):
        data = {"job_id": str(self.job_id), "status": "stop"}
        message = json.dumps(data)
        self.sendTo.send(message)
        self.cleanup()

    def run(self, toServer):
        if self.processGraph != None:
            try:
                timeStart = str(datetime.now())
                common.logMessage(logging.INFO, 'started job_id: ' + self.job_id + "with name: " + self.title,common.process_user)
                outputinfo = self.processGraph.run(self, toServer, self.fromServer)
                timeEnd = str(datetime.now())
                if 'spatialextent' in outputinfo:
                    self.spatialextent = outputinfo['spatialextent']
                log = {'type' : 'progressevent', 'job_id': self.job_id, 'progress' : 'job finished' , 'last_updated' : timeEnd, 'status' : constants.STATUSJOBDONE}   
                toServer.put(log)
                ##self.sendTo.close()
                ##self.fromServer.close()
                if outputinfo != None:
                    if outputinfo['status'] == constants.STATUSSTOPPED:
                        self.cleanup()
                    else:
                        self.status = constants.STATUSJOBDONE                      
                else:                    
                    self.status = constants.STATUSJOBDONE 
                path = common.openeoip_config['data_locations']['root_user_data_location']
                path = os.path.join(path['location'] + '/' + str(self.job_id + "/jobmetadata.json") )                                   
                dict = self.toDict(False) 
                dict['start_datetime']  = timeStart
                dict['end_datetime']  = timeEnd
                with open(path, "w") as fp:
                    json.dump(dict, fp)   
                common.logMessage(logging.INFO,'finished job_id: ' + self.job_id ,common.process_user)
            except  (Exception, BaseException, customexception.CustomException) as ex:
                timeEnd = str(datetime.now()) 
                code = ''               
                if isinstance(ex, customexception.CustomException):
                    code = ex.code
                    message = ex.message
                else:                    
                    message = 'failed job_id: ' + self.job_id + " with error " + str(ex)
                log = {'type' : 'progressevent', 'job_id': self.job_id, 'progress' : 'job finished' , 'last_updated' : timeEnd, 'status' : constants.STATUSERROR, 'message': message, 'code': code}   
                toServer.put(log)
                common.logMessage(logging.ERROR,message,common.process_user)    
    
  
        
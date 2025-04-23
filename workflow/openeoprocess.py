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
import os, shutil
from openeooperation import put2Queue
import customexception
import datacube
import ilwis
import re
import operations.operationconstants as opc

def get(key,values,  defaultValue):
    if key in values:
        return values[key]
    return defaultValue

# helper class for the process graph to store the passed parameter values(process) and use
# them while executing the process
class OpenEOParameter:
    schema = {}
    name = ''
    description = ''
    optional = False
    deprecated = False
    experimental = False
    default = None
    returnValue = None
    parameters = []
    spatial_organization = []
    datatype = None
    datacube = None    
   
    def __init__(self, parm=None):
        if parm == None:
            return
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
            dimensions = parm[constants.DIMENSIONSLABEL]
            self.spatial_organization = []
            for dim in dimensions:
                tp = dim['type']
                if tp == 'spatial':
                    self.spatial_organization.append((tp, get('axis', dimensions, '')))
                if tp == 'geometry':
                    self.spatial_organization.append((tp, get('geometry', dimensions, '')))
                if tp in ['eo:bands', 'temporal', 'other']:
                    self.spatial_organization.append(tp, tp)
    def __eq__(self, other):
        if not isinstance(other, OpenEOParameter):
            return False
        schema_equal = self.schema == other.schema
        return (schema_equal and
                self.name == other.name and
                self.description == other.description and
                self.optional == other.optional and
                self.deprecated == other.deprecated and
                self.experimental == other.experimental and
                self.default == other.default and   
                self.returnValue == other.returnValue and
                self.parameters == other.parameters and
                self.spatial_organization == other.spatial_organization and
                self.datatype == other.datatype and
                self.datacube == other.datacube)
    
    
   
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


# any processing request will be started as an OpenEOProcess in a seperate process in python
# the main activity is running the process graph(see the 'run' method). The rest of the attributes of this class
# exist for adminstrative and management purposes.
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
        # from here basically parse the http request and store its values in the class instance
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

    # starts the validation of a process graph. This can only happen after the OpenEOProcess has been
    # created and a process graph has been created
    def validate(self):
        errorsdict = []
        errors = self.processGraph.validateGraph()
        for error in errors:
            errorsdict.append({ "id" : self.job_id, "code" : "missing operation", "message" : error})
            
        return errorsdict             

    # helper function for the toDict method. ensure that only existing attributes are used
    def setItem(self, key, dict):
        if hasattr(self, key):
            dict[key] = getattr(self, key)
        return dict
     # helper function for the toDict method. ensure that only existing attributes are used and a default 
     # can be used if neeeded
    def setItem2(self, key, dict, alt):
        if hasattr(self, alt):
            dict[key] = getattr(self, alt)
        return dict

    def estimate(self, user):
        return self.processGraph.estimate()

    # translates the info of this class to a dict. This dict is the basis for the metadata file that is dumped
    # in the output folder.
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

            if self.returns != {}:
                outInfo = self.returns
                if outInfo['datatype'] != constants.DTRASTER:
                    subtype = 'string'
                    schema = opc.OPERATION_SCHEMA_STRING
                else:
                    subtype = 'datacube'
                    schema = opc.OPERATION_SCHEMA_DATACUBE
                processDict['results'] = { 'subtype' : subtype, 'schema' : schema}
            else:        
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
     
    # removes all results of a certain job and removes the folder
    def cleanup(self):
        filePath = common.openeoip_config['data_locations']['root_user_data_location']
        filePath = filePath['location'] + '/' + str(self.job_id)  
        if os.path.isdir(filePath):
            shutil.rmtree(filePath)
  
    def stop(self):
        data = {"job_id": str(self.job_id), "status": "stop"}
        message = json.dumps(data)
        self.sendTo.send(message)
        self.cleanup()

    def saveResult(self, path, data, format):
        count = 0
        env = ilwis.Envelope()
        if data != None:
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, datacube.DataCube):
                        name = d['title'] 
                        name = name.replace('_ANONYMOUS', 'raster')                    
                        for raster in d.getRasters():
                            envTemp = raster.envelope()
                            outpath = path + '/' + name + "_"+ str(count)
                            raster.store("file://" + outpath, format, "gdal")
                            count = count + 1
                            if not env:
                                env = envTemp
                            else:
                                env.add(envTemp)                    
                parts = re.split("[\s,]+", str(env))
                return parts
    
    # executes the process graph and is the end point for all raised exceptions that occur while running
    # the process graph.
    def run(self, toServer):
        """
        Executes the process graph and handles all raised exceptions during execution.

        Args:
            toServer: The server object for communication.
        """
        if self.processGraph is not None:
            try:
                time_start = str(datetime.now())
                self._logJobStart(time_start)

                # Start the process graph
                output_info = self.processGraph.run(self, toServer, self.fromServer)

                time_end = str(datetime.now())
                self._handleProcessGraphOutput(output_info, toServer, time_start, time_end)

            except (Exception, BaseException, customexception.CustomException) as ex:
                self._handleRunException(ex, toServer)

    def _logJobStart(self, time_start):
        """
        Logs the start of the job.

        Args:
            time_start: The start time of the job.
        """
        common.logMessage(
            logging.INFO,
            f"started job_id: {self.job_id} with name: {self.title}",
            common.process_user
        )

 
    def _handleProcessGraphOutput(self, output_info, toServer, time_start, time_end):
        """
        Handles the output of the process graph.

        Args:
            output_info: The output information from the process graph.
            toServer: The server object for communication.
            time_start: The start time of the job.
            time_end: The end time of the job.
        """
        if 'spatialextent' in output_info:
            self.spatialextent = output_info['spatialextent']
        self.returns = output_info

        log = {
            'type': 'progressevent',
            'job_id': self.job_id,
            'progress': 'job finished',
            'last_updated': time_end,
            'status': constants.STATUSJOBDONE,
            'current_operation': '?'
        }
        toServer.put(log)

        if output_info is not None:
            if output_info['status'] == constants.STATUSSTOPPED:
                self.cleanup()
            else:
                self.status = constants.STATUSJOBDONE
        else:
            self.status = constants.STATUSJOBDONE

        self._saveMetadata(output_info, time_start, time_end)

    def _saveMetadata(self, output_info, time_start, time_end):
        """
        Saves metadata about the finished process graph.

        Args:
            output_info: The output information from the process graph.
            time_start: The start time of the job.
            time_end: The end time of the job.
        """
        path = common.openeoip_config['data_locations']['root_user_data_location']
        filedir = os.path.join(path['location'], str(self.job_id))
        metadata_path = os.path.join(filedir, "jobmetadata.json")

        metadata = self.toDict(False)
        metadata['start_datetime'] = time_start
        metadata['end_datetime'] = time_end

        if not os.path.exists(filedir):
            os.makedirs(filedir)
            if output_info['datatype'] == constants.DTRASTER:
                metadata['spatialextent'] = self.saveResult(filedir, output_info['value'], "GTiff")

        with open(metadata_path, "w") as fp:
            json.dump(metadata, fp)

        common.logMessage(logging.INFO, f"finished job_id: {self.job_id}", common.process_user)

    def _handleRunException(self, ex, toServer):
        """
        Handles exceptions raised during the execution of the process graph.

        Args:
            ex: The exception that was raised.
            toServer: The server object for communication.
        """
        time_end = str(datetime.now())
        code = ''
        if isinstance(ex, customexception.CustomException):
            code = ex.jsonErr['code']
            message = ex.jsonErr['message']
        else:
            message = f"failed job_id: {self.job_id} with error {str(ex)}"

        log = {
            'type': 'progressevent',
            'job_id': self.job_id,
            'progress': 'job finished',
            'last_updated': time_end,
            'status': constants.STATUSERROR,
            'message': message,
            'code': code,
            'current_operation': '?'
        }
        toServer.put(log)
        common.logMessage(logging.ERROR, message, common.process_user)

    def _logJobStart(self, time_start):
        """
        Logs the start of the job.

        Args:
            time_start: The start time of the job.
        """
        common.logMessage(
            logging.INFO,
            f"started job_id: {self.job_id} with name: {self.title}",
            common.process_user
        )

    def _handleProcessGraphOutput(self, output_info, toServer, time_start, time_end):
        """
        Handles the output of the process graph.

        Args:
            output_info: The output information from the process graph.
            toServer: The server object for communication.
            time_start: The start time of the job.
            time_end: The end time of the job.
        """
        if 'spatialextent' in output_info:
            self.spatialextent = output_info['spatialextent']
        self.returns = output_info

        log = {
            'type': 'progressevent',
            'job_id': self.job_id,
            'progress': 'job finished',
            'last_updated': time_end,
            'status': constants.STATUSJOBDONE,
            'current_operation': '?'
        }
        toServer.put(log)

        if output_info is not None:
            if output_info['status'] == constants.STATUSSTOPPED:
                self.cleanup()
            else:
                self.status = constants.STATUSJOBDONE
        else:
            self.status = constants.STATUSJOBDONE

        self._saveMetadata(output_info, time_start, time_end)        

    
  
        
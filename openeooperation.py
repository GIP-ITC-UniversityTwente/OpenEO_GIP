import threading
import json
from constants import constants
import ilwis
from rasterdata import *
import logging
import common
import customexception


operations1 = {}


def put2Queue(processOutput, content):
    if processOutput != None: ##synchronous calls dont have output queues
        processOutput.put(content)  

def message_handler(operation, processInput):

    while True:
        if processInput.closed == False:        
            if processInput.poll(timeout=1):
                    try:
                        data = processInput.recv()
                        message = json.loads(data)
                        if 'status' in message:
                            status = message['status']
                            if status == 'stop':
                                operation.stopped = True
                                return ## end thread
                    except Exception: 
                        return                           



class OpenEoOperation:
    name = ''
    summary = ''
    description = ''
    categories = []
    inputParameters = {}
    outputParameters = {}
    exceptions = {}
    examples = []
    links = []
    runnable = False 
    stopped = False

    def startListener(self, processInput):
        if processInput != None: ## synchronous calls dont have listeners
            message_thread = threading.Thread(target=message_handler,args=(self, processInput,) )
            message_thread.start()

    def loadOpenEoJsonDef(self, filename):
        jsondeffile = open('./operations/metadata/' + filename)
        jsondef = json.load(jsondeffile)

        self.name = jsondef['id']
        self.description = jsondef['description']
        self.summary = jsondef['summary']
        self.categories = jsondef['categories']
        if 'exceptions'in jsondef:
            for ex in jsondef['exceptions'].items():
                self.exceptions[ex[0]] = ex[1]['message']

        for parm in jsondef['parameters']:
          self.addInputParameter(parm['name'], parm['description'], parm['schema'])

        if 'returns' in jsondef:
            self.addOutputParameter(jsondef['returns']['description'],jsondef['returns']['schema'])

        if 'examples' in jsondef:
            for ex in jsondef['examples']:
                self.examples.append(str(ex))

        if 'links' in jsondef:
            for lnk in jsondef['links']:
                self.addLink(lnk)
      
    def toDict(self):
        iparameters = []
        for value in self.inputParameters.values():
            iparameters.append( { 'name' : value['name'], 'description' : value['description'], 'schema' : value['schema']})   

        operationDict = { 'id' : self.name , 
                    'description' : self.description, 
                    'summary' : self.summary,
                    'parameters' : iparameters,
                    'returns' : self.outputParameters,
                    'categories' : self.categories,
                    'exceptions' : self.exceptions,
                    'examples' : self.examples
                    }

        return operationDict
    
    def addInputParameter(self, name, description, schema):
        self.inputParameters[name] = {'name' : name, 'description' : description, 'schema' : schema}

    def addOutputParameter(self, description, schema):
        self.outputParameters['description'] = description
        self.outputParameters['schema'] = schema

    def addLink(self, link):       
        self.links.append(link)

    def setArguments(self, graph, inputDict ):
        isRoot = type(graph) is dict
        if isRoot:
             g = graph[next(iter(graph))]
        else:
             g = graph[1]   

        newGraph= {}     
        for graphItem in graph.items():
            if graphItem[0] in inputDict:
                newGraph.update({ graphItem[0] :  inputDict[graphItem[0]]})

            else :
                if type(graphItem[1]) is dict:
                    setItem = self.setArguments(graphItem, inputDict)
                    if setItem != None:
                        newGraph[graphItem[0]] = setItem
                else:                     
                    newGraph[graphItem[0]] = graphItem[1]

        if isRoot: ## we are back at the root
             newGraph.update(setItem)
             d = {}
             d[next(iter(graph))] = newGraph
             return d
        
        return newGraph

    def createOutput(self, idx, ilwisRaster, extra):
        rasterData = RasterData()
        rasterData.fromRasterCoverage(ilwisRaster, extra )
        return rasterData

    def collectRasters(self, rasters):
        if len(rasters) == 1:
            return rasters[0]
        
        ilwisRaster = self.createNewRaster(rasters)

        for index in range(0, len(rasters)):
            iter = rasters[index].begin()
            ilwisRaster.addBand(index, iter)

        return ilwisRaster

    def createNewRaster(self, rasters):
        stackIndexes = []
        for index in range(0, len(rasters)):
            stackIndexes.append(index)

        dataDefRaster = rasters[0].datadef()

        for index in range(0, len(rasters)):
            dfNum = rasters[index].datadef()
            dataDefRaster = ilwis.DataDefinition.merge(dfNum, dataDefRaster)

        grf = ilwis.GeoReference(rasters[0].coordinateSystem(), rasters[0].envelope() , rasters[0].size())
        rc = ilwis.RasterCoverage()
        rc.setGeoReference(grf) 
        rc.setSize(ilwis.Size(rc.size().xsize, rc.size().ysize, 0))
        dom = ilwis.NumericDomain("code=integer")
        #rc.setStackDefinition(dom, stackIndexes)
        rc.setDataDef(dataDefRaster)
     
       # for index in range(0, len(rasters)):
       #     rc.setBandDefinition(index, rasters[index].datadef())

        return rc            

    def createExtra(self, r, idx):
        att = {'type' : 'float'}
        att = {'name' : 'calculated band ' + str(idx)}
        att = {'details' : {}}
        self.extra = { 'temporalExtent' : r.temporalExtent, 'bands' : [att], 'epsg' : r.epsg}
     
    def checkSpatialDimensions(self, rasters):
        pixelSize = 0
        allSame = True 
        extent = []       
        for rc in rasters:
            if pixelSize == 0:
                pixelSize = rc.getRaster().pixelSize()
            else:
                if allSame:
                    allSame = pixelSize == rc.getRaster().pixelSize()
                spExtentTarget = rc.spatialExtent
                if len(extent) == 0:
                    extent = rc.spatialExtent
                else:  
                    extentTest = rc.spatialExtent                  
                    allSame = {
                            extent[0] == extentTest[0] and  
                            extent[1] == extentTest[1] and 
                            extent[2] == extentTest[2] and
                            extent[3] == extentTest[3]} 
            if not allSame:
                break
                                                   
        return allSame 

    def setOutput(self, ilwisRasters, extra):
        outputRasters = []
        if len(ilwisRasters) > 0:
            if self.rasterSizesEqual:
                ilwisRaster = self.collectRasters(ilwisRasters)
                outputRasters.append(self.createOutput(0, ilwisRaster, extra))
            else:
                count = 0
                for rc in self.rasters:
                    outputRasters.append(self.createOutput(count, rc, extra))
                    count = count + 1

        return outputRasters 

    def constructExtraParams(self, raster, temporalExtent, index):
         bands = []
         bands.append(raster.index2band(index))
         extra = { 'temporalExtent' : temporalExtent, 'bands' : bands, 'epsg' : raster.epsg} 

         return extra
    
    def logProgress(self, processOutput, job_id, message,  status):
        timenow = str(datetime.now())
        log = {'type' : 'progressevent', 'job_id': job_id, 'progress' : message , 'last_updated' : timenow, 'status' : status}   
        put2Queue(processOutput, log)
    
    def logStartOperation(self, processOutput,openeojob, extraMessage=""):
        common.logMessage(logging.INFO, 'started: ' + self.name + " with job name:" + openeojob.title,common.process_user)
        if extraMessage == "":
            return self.logProgress(processOutput, openeojob.job_id, self.name ,constants.STATUSRUNNING)
        else:
            return self.logProgress(processOutput, openeojob.job_id, self.name + ": " + extraMessage ,constants.STATUSRUNNING)

    def logEndOperation(self, processOutput,openeojob, extraMessage=""):
        common.logMessage(logging.INFO, 'ended: ' + self.name + " with job name:" + openeojob.title, common.process_user)
        if extraMessage == "":
            return self.logProgress(processOutput, openeojob.job_id, 'finished ' + self.name,constants.STATUSFINISHED)
        else:
            return self.logProgress(processOutput, openeojob.job_id, 'finished ' + self.name +": " + extraMessage,constants.STATUSFINISHED)
    
    def handleError(self, processOutput, job_id, parameter, message, code):
        self.logProgress(processOutput, job_id, message, constants.STATUSERROR )
        raise customexception.CustomException(code, job_id, parameter, message)


            

def createOutput(status, value, datatype, format='')        :
    return {"status" : status, "value" : value, "datatype" : datatype, 'format' : format}  

def messageProgress(processOutput, job_id, progress) :
    if processOutput != None:
        processOutput.put({'type': 'progressevent','progress' : progress, 'job_id' : job_id, 'status' : constants.STATUSRUNNING}) 



                







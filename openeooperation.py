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

    def createOutput(self, idx, ilwisRasters, extra):
        rasterData = RasterData()
        rasterData.load(ilwisRasters, 'ilwisraster', extra )
        if 'name' in extra:
            rasterData['title'] = extra['name']
        return rasterData

    def collectRasters(self, rasters):
        if len(rasters) == 1:
            return rasters[0]
        
        ilwisRaster = self.createNewRaster(rasters)

        for index in range(0, len(rasters)):
            iter = rasters[index].begin()
            ilwisRaster.addBand(index, iter) ## will add to the end
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
        rc.setSize(ilwis.Size(rc.size().xsize, rc.size().ysize, len(rasters)))
        dom = ilwis.NumericDomain("code=integer")
        rc.setStackDefinition(dom, stackIndexes)
        rc.setDataDef(dataDefRaster)
     
        for index in range(0, len(rasters)):
           rc.setBandDefinition(index, rasters[index].datadef())

        return rc
               
    def getImplementationLevel(self, r : RasterData):
        key = next(iter(r['rasters'])) # from the key we can learn at which level the implementation can be found
        parts = key.split(':')
        level = r['dimStructure'][len(parts)-1]
        return level

    def createExtra(self, r : RasterData, reduce=False):
        level = self.getImplementationLevel(r)
        meta = r['dimMetadata'][level]
        bands = []
        if  level == constants.DIMSPECTRALBANDS:
            for b in meta:
                bands.append({'type' : 'float', 'name' : 'calculated from ' + b,'details' : {} } )

        self.extra = { 'temporalExtent' : r['temporalExtent'], 'bands' : bands, 'epsg' : r['proj:epsg']}
        if reduce: # we cut out the implementation level(=reduced) as this is now become one level higher
            self.extra['dimStructure'] = []
            implLevelIndex = r['dimStructure'].index(level)
            for idx in range(len(r['dimStructure'])):
                if implLevelIndex + 1 != idx:
                    self.extra['dimStructure'].append(r['dimStructure'][idx])
            self.extra['textsublayers'] = {}                    
        else:                    
            self.extra['dimStructure'] = r['dimStructure']
            self.extra['textsublayers'] = r.getLayers()

        self.extra['rasterkeys'] = r['rasters'].keys()
        self.extra['basename'] = self.name
     
    def checkSpatialDimensions(self, rasters):
        pixelSize = 0
        allSame = True 
        extent = []       
        for rc in rasters:
            for raster in rc['rasters'].values():
                if pixelSize == 0:
                    pixelSize = rc.getRaster().geoReference().pixelSize()
                    extent = rc['spatialExtent']               
                else:
                    allSame = pixelSize == rc.getRaster().geoReference().pixelSize()
                    extentTest = rc['spatialExtent']                 
                    allSame = allSame and \
                        extent[0] == extentTest[0] and \
                        extent[1] == extentTest[1] and \
                        extent[2] == extentTest[2] and \
                        extent[3] == extentTest[3] 
                if not allSame:
                    break
                                                   
        return allSame 
    
    def makeOutput(self, ilwisRasters, extra):
        outputRasters = []
        rasterData = RasterData()
        rasterData.load(ilwisRasters, 'ilwisraster', extra )
        outputRasters.append(rasterData)
        return outputRasters 
    
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
    
    def mapname(self, name):
        namemapping = {'t' : constants.DIMTEMPORALLAYER, 'bands' : constants.DIMSPECTRALBANDS}
        if name in namemapping:
            return namemapping[name]
        return name
    
    def findRasterData(self, toServer, job_id, rasterData, arguments):
        arrIndex = -1
        dimName = self.mapname(arguments['dimensions']['resolved'])
        if 'index' in arguments:
            arrIndex = arguments['index']['resolved'] 
            if dimName in rasterData['dimStructure']:
                meta = rasterData['dimMetadata'][dimName]
                if len(meta) <= arrIndex:
                    self.handleError(toServer, job_id, 'band index',"Number of raster bands doesnt match given index", 'ProcessParameterInvalid')
            return arrIndex
                        
        if 'label' in arguments:
            for idx in range(len(rasterData)):
                meta = rasterData['dimMetadata'][dimName]
                idx = 0
                for key in meta:
                    if key == arguments['label']['resolved']:
                        return idx
                    idx = idx + 1
        return arrIndex
    
    

    def constructExtraParams(self, raster, temporalExtent, index):
         bands = []
         band = raster.index2band(index)
         if band == None:
             band = {'name' : self.name, 'details' : {}}
         bands.append(band)
         extra = { 'temporalExtent' : temporalExtent, 'bands' : bands, 'epsg' : raster['proj:epsg'], 'details': bands[0]['details'], 'name' : bands[0]['name']} 

         return extra
    
    def logProgress(self, processOutput, job_id, message,  status, progress=0):
        timenow = str(datetime.now())
        log = {'type' : 'progressevent', 'job_id': job_id, 'progress' : message , 'last_updated' : timenow, 'status' : status, 'progress' : progress, 'current_operation' : self.name}   
        put2Queue(processOutput, log)
    
    def logStartOperation(self, processOutput,openeojob, extraMessage=""):
        common.logMessage(logging.INFO, 'started: ' + self.name + " with job name:" + openeojob.title,common.process_user)
        if extraMessage == "":
            return self.logProgress(processOutput, openeojob.job_id, self.name ,constants.STATUSRUNNING, 0)
        else:
            return self.logProgress(processOutput, openeojob.job_id, self.name + ": " + extraMessage ,constants.STATUSRUNNING, 0)

    def logEndOperation(self, processOutput,openeojob, extraMessage=""):
        common.logMessage(logging.INFO, 'ended: ' + self.name + " with job name:" + openeojob.title, common.process_user)
        if extraMessage == "":
            return self.logProgress(processOutput, openeojob.job_id, 'finished ' + self.name,constants.STATUSFINISHED,100)
        else:
            return self.logProgress(processOutput, openeojob.job_id, 'finished ' + self.name +": " + extraMessage,constants.STATUSFINISHED,100)
    
    def handleError(self, processOutput, job_id, parameter, message, code):
        message = message + ": " + self.name
        self.logProgress(processOutput, job_id, message, constants.STATUSERROR )
        raise customexception.CustomException(code, job_id, parameter, message)
    
    def resample(self,targetRaster, sourceRaster):
        inputRaster = targetRaster.getRaster().rasterImp() 
        env = inputRaster.envelope()
        pixSize = targetRaster.getRaster().pixelSize()
        projection = 'epsg:' + str(targetRaster.epsg)
        csy = ilwis.CoordinateSystem(projection)
        grf = ilwis.do('createcornersgeoreference', \
                           env.minCorner().x, env.minCorner().y, env.maxCorner().x, env.maxCorner().y, \
                           pixSize, csy, True, '.')
        rm = sourceRaster.getRaster().rasterImp()
        outputRc = ilwis.do("resample", rm, grf, 'nearestneighbour')
        self.extra = self.constructExtraParams(targetRaster, targetRaster.temporalExtent, 0)  
        return self.setOutput([outputRc], self.extra)[0]
    
    def type2type(self, a):
        t = DTUNKNOWN
        if a == None:
            return t
        if isinstance(a, int):
            t = DTINTEGER
        elif  isinstance(a, float):                    
            t = DTFLOAT
        elif  isinstance(a, str):
            t = DTSTRING                    
        elif  isinstance(a, list):
            t == DTLIST
        elif  isinstance(a, bool):
            t == DTBOOL 
        elif  isinstance(a, dict):
            t == DTDICT 
        elif  isinstance(a, RasterData):
            t == DTRASTER 
        return t                            

    def getMandatoryParam(self, toServer, job_id, args, key):
        if key in args:
            return args[key]['resolved']
        self.handleError(toServer, job_id, "parameter", "missing parameter:" + key, 'ProcessParameterInvalid')
            

def createOutput(status, value, datatype, format='')        :
    return {"status" : status, "value" : value, "datatype" : datatype, 'format' : format}  

def messageProgress(processOutput, job_id, progress) :
    if processOutput != None:
        processOutput.put({'type': 'progressevent','progress' : progress, 'job_id' : job_id, 'status' : constants.STATUSRUNNING, 'current_operation' : 'dummy'}) 



                







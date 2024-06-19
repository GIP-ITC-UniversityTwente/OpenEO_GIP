import threading
import json
from constants import constants
import ilwis
from rasterdata import *
import logging
import common
import customexception


operations1 = {}

def parse_rectangle(rect_str):
    # Convert the string to a list of integers
    coords = list(map(float, rect_str.split()))
    # Ensure the coordinates are in the form [(x1, y1), (x2, y2)]
    return [(coords[0], coords[1]), (coords[2], coords[3])]

def overlaps(rect1_str, rect2_str):
    # Parse the input strings to get the rectangle coordinates
    rect1 = parse_rectangle(rect1_str)
    rect2 = parse_rectangle(rect2_str)
    
    # Unpack the rectangles
    (x1, y1), (x2, y2) = rect1
    (x3, y3), (x4, y4) = rect2
    
    # Check if one rectangle is to the left of the other
    if x2 < x3 or x4 < x1:
        return False
    
    # Check if one rectangle is above the other
    if y2 < y3 or y4 < y1:
        return False
    
    return True

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
        key = next(iter(r[DATAIMPLEMENTATION])) # from the key we can learn at which level the implementation can be found
        parts = key.split(':')
        level = r[STRUCTUREDEFDIM][len(parts)-1]
        return level

    def createExtra(self, r : RasterData, reduce=False, basename=''):
        level = self.getImplementationLevel(r)
        meta = r[METADATDEFDIM][level]
        bands = []
        rasterKeys = []
        bname = basename if basename != '' else 'calc_from'
        if  level == constants.DIMSPECTRALBANDS:
            count = 0
            for b in meta['items']:
                bands.append({'type' : 'float', 'bandIndex' : count, 'name' : basename + '_b' + str(count),'details' : {} } )
   
                count = count + 1
        for key in r[DATAIMPLEMENTATION]:
            rasterKeys.append(key)
        self.extra = { 'temporalExtent' : r['temporalExtent'], 'bands' : bands, 'epsg' : r['proj:epsg'], 'rasterkeys': rasterKeys }
        if reduce: # we cut out the implementation level(=reduced) as this is now become one level higher
            self.extra[STRUCTUREDEFDIM] = []
            implLevelIndex = r[STRUCTUREDEFDIM].index(level)
            for idx in range(len(r[STRUCTUREDEFDIM])):
                if implLevelIndex + 1 != idx:
                    self.extra[STRUCTUREDEFDIM].append(r[STRUCTUREDEFDIM][idx])
            self.extra['textsublayers'] = {} 
            self.extra['rasterkeys'] = []
            for key in r[DATAIMPLEMENTATION].keys(): # remove the last index as we have reduce the dimension
                parts = key.split(':')
                newkey = key if len(parts) == 1 else key.rsplit(':', 1)[0] #len == 1 is sp3ecial case as this is the root; the removed dim is implicit in the ilwraster
                self.extra['rasterkeys'].append(newkey)                             

        else:                    
            self.extra[STRUCTUREDEFDIM] = r[STRUCTUREDEFDIM]
            self.extra['textsublayers'] = r.getLayers()
            self.extra[DATAIMPLEMENTATION] = r[DATAIMPLEMENTATION].keys()

        
        self.extra['basename'] = self.name
     
    def checkSpatialDimensions(self, rasters):
        pixelSize = 0
        allSame = True 
        extent = []       
        for rc in rasters:
            for raster in rc[DATAIMPLEMENTATION].values():
                if pixelSize == 0:
                    pixelSize = rc.getRaster().geoReference().pixelSize() #first raster
                    extent = rc['spatialExtent']               
                else:
                    allSame = pixelSize == raster.geoReference().pixelSize() #other rasters
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
    
    def getDimension(self, raster : RasterData, arguments):
        if 'dimensions' in arguments:
            self.mapname(arguments['dimensions']['resolved'])
        else: # if the dimension is not given we assume the toplevel
            return raster[STRUCTUREDEFDIM][0]
    
    def findRasterData(self, toServer, job_id, rasterData, arguments):
        arrIndex = -1
        dimName = self.getDimension(rasterData,arguments)
        if 'index' in arguments:
            arrIndex = arguments['index']['resolved'] 
            if dimName in rasterData[STRUCTUREDEFDIM]:
                meta = rasterData[METADATDEFDIM][dimName]
                if len(meta) <= arrIndex:
                    self.handleError(toServer, job_id, 'band index',"Number of raster bands doesnt match given index", 'ProcessParameterInvalid')
            return arrIndex
                        
        if 'label' in arguments:
            for idx in range(len(rasterData)):
                meta = rasterData[METADATDEFDIM][dimName]
                idx = 0
                for key in meta:
                    if key == arguments['label']['resolved']:
                        return idx
                    idx = idx + 1
        return arrIndex
    
    

    def constructExtraParams(self, raster, temporalExtent, indexes):
         bands = []
         for idx in indexes:
            band = raster.index2band(idx)
            if band == None:
                band = {'name' : self.name + "_band_" + str(idx), 'details' : {}}
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



                







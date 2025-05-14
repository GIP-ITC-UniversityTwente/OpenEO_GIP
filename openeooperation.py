import threading
import json
from constants import constants
import ilwis
from datacube import *
import logging
import common
import customexception
from pathlib import Path
from urllib.parse import urlparse, unquote


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
        rasterData = DataCube()
        rasterData.load(ilwisRasters, 'ilwisraster', extra )
        if 'name' in extra:
            rasterData['title'] = extra['name']
        return rasterData

    def collectRasters(self,job_id, rasters):
        if len(rasters) == 1:
            return rasters[0]
        
        ilwisRaster = self.createNewRaster( rasters)
     
        for index in range(0, len(rasters)):
            iter = rasters[index].begin()
            ilwisRaster.addBand(index, iter) ## will add to the end
        common.registerIlwisIds(job_id, ilwisRaster)                
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
               
    

    def createExtra(self, r : DataCube, reduce=False, basename=''):
        level = r.getImplementationDimension()
        meta = r[DIMENSIONSLABEL][level]
        bands = []
        rasterKeys = []
        bname = basename if basename != '' else 'calc_from'
        if  level == constants.DIMSPECTRALBANDS:
            count = 0
            for b in meta:
                bands.append({'type' : 'float', BANDINDEX : count, 'name' : basename + '_b' + str(count),'details' : {} } )
   
                count = count + 1
        self.extra = { TEMPORALEXTENT : r[TEMPORALEXTENT], 'bands' : bands, 'epsg' : r['proj'], 'rasterkeys': rasterKeys }
        if reduce: # we cut out the implementation level(=reduced) as this is now become one level higher
            self.extra = {}
            self.extra['textsublayers'] = {}
            self.extra = { TEMPORALEXTENT : r[TEMPORALEXTENT],  'bands' : bands, 'epsg' : r['proj'], 'rasterkeys': rasterKeys } 
        else:                    
            self.extra['textsublayers'] = r.getLayersTempExtent()
            self.extra['data'] = r.getRasters()

        
        self.extra['basename'] = self.name
     
    def checkSpatialDimensions(self, rasters):
        pixelSize = 0
        allSame = True 
        extent = []       
        for rc in rasters:
            data = rc.getRasters()
            for raster in data:
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
    
  
                   
        
    def makeOutput(self,  ilwisRasters, extra):
        outputRasters = []
        rasterData = DataCube()
        rasterData.load(ilwisRasters, 'ilwisraster', extra )
        outputRasters.append(rasterData)
        return outputRasters 
    
    def setOutput(self, job_id, ilwisRasters, extra):
        outputRasters = []
        if len(ilwisRasters) > 0:
            is4D = ilwisRasters[0].size().zsize > 1
            if self.rasterSizesEqual and not is4D:
                ilwisRaster = self.collectRasters(job_id, ilwisRasters)
                outputRasters.append(self.createOutput(0, ilwisRaster, extra))
            else:
                count = 0
                for rc in ilwisRasters:
                    outputRasters.append(self.createOutput(count, rc, extra))
                    count = count + 1

        return outputRasters 
    
    def mapname(self, name):
        namemapping = {'t' : constants.DIMTEMPORALLAYER, 'bands' : constants.DIMSPECTRALBANDS}
        if name in namemapping:
            return namemapping[name]
        return name
    
    def getDimension(self, raster : DataCube, arguments):
        if DIMENSIONSLABEL in arguments:
            return self.mapname(arguments[DIMENSIONSLABEL]['resolved'])
        else: # if the dimension is not given we assume the toplevel
            return raster.getDimension()
    
    def findBandIndex(self, toServer, job_id, rasterData, arguments):
        arrIndex = -1
        dimName = self.getDimension(rasterData,arguments)
        if 'index' in arguments:
            arrIndex = arguments['index']['resolved'] 
            if dimName in rasterData[DIMENSIONSLABEL]:
                meta = rasterData[DIMENSIONSLABEL][dimName]
                for dimItem in meta:
                    if BANDINDEX in dimItem:
                        if dimItem[BANDINDEX] == arrIndex:
                            return dimItem[BANDINDEX]
            return -1
                        
        if 'label' in arguments:
            for idx in range(len(rasterData)):
                meta = rasterData[DIMENSIONSLABEL][dimName]
                for dimItem in meta:
                    if 'label' in dimItem:
                        if dimItem['label'].lower() == arguments['label']['resolved'].lower():
                            return dimItem[BANDINDEX]
                    if 'commonbandname' in dimItem:
                        # strictly speaking this should not be working but for users the differences between label and commonbandname is fairly vague. so...
                        if dimItem['commonbandname'].lower() == arguments['label']['resolved'].lower():
                            return dimItem[BANDINDEX] 
        return arrIndex
    
    

    def constructExtraParams(self, raster, temporalExtent, indexes):
         bands = []
         for idx in indexes:
            band = raster.index2band(idx)
            if band == None:
                band = {'name' : self.name + "_band_" + str(idx), 'details' : {}}
            bands.append(band)
         extra = { TEMPORALEXTENT : temporalExtent, 'bands' : bands, 'epsg' : raster['proj'], 'details': bands[0]['details'], 'name' : bands[0]['name']} 

         return extra
    
    def getDefaultArgs(self, arguments):
          toServer = ''
          job_id = ''
          if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']
          return toServer, job_id
                
    def logProgress(self, processOutput, job_id, message,  status, progress=0, ids = []):
        timenow = str(datetime.now())
        log = {'type' : 'progressevent', 'job_id': job_id, 'message' : message , 'last_updated' : timenow, 'status' : status, 'progress' : progress, 'current_operation' : self.name, 'objectids' : ids }   
        put2Queue(processOutput, log)
    
    def logStartPrepareOperation(self, jobid):
        common.logMessage(logging.INFO, self.name + " started prepare. job name:" + jobid,common.process_user)

    def logEndPrepareOperation(self, jobid):
        common.logMessage(logging.INFO, self.name + " ended prepare. with job name:" + jobid,common.process_user)        

    def logStartOperation(self, processOutput,openeojob, extraMessage=""):
        common.logMessage(logging.INFO, self.name + " started run. with job name:" + openeojob.title,common.process_user)
        if extraMessage == "":
            return self.logProgress(processOutput, openeojob.job_id, self.name ,constants.STATUSRUNNING, 0)
        else:
            return self.logProgress(processOutput, openeojob.job_id, "started " + self.name + ": " + extraMessage ,constants.STATUSRUNNING, 0)

    def logEndOperation(self, processOutput,openeojob, outputs = None, extraMessage=""):
        common.logMessage(logging.INFO,self.name + " ended run. with job name:" + openeojob.title, common.process_user)
        if extraMessage == "":
            return self.logProgress(processOutput, openeojob.job_id, 'finished ' + self.name,constants.STATUSFINISHED,100, common.getIdsForJob(openeojob.job_id))
        else:
            return self.logProgress(processOutput, openeojob.job_id, 'finished ' + self.name +": " + extraMessage,constants.STATUSFINISHED,100, common.getIdsForJob(openeojob.job_id))
    
    def handleError(self, processOutput, job_id, parameter, message, code):
        common.logMessage(logging.ERROR, 'error: ' + self.name + " with job:" + job_id + " ;" + message, common.process_user)
        message = message + ": " + self.name
        self.logProgress(processOutput, job_id, message, constants.STATUSERROR )
        raise customexception.CustomException(code, job_id, parameter, message)
    
    def checkOverlap(self, toServer, job_id, envCube, envMap):
        b3 = envMap.overlap(envCube)
        if not  bool(b3):
            self.handleError(toServer, job_id, 'extents','extents given and extent data dont overlap', 'ProcessParameterInvalid')    

    # checks if extents are valid. must contain 'north', 'south', 'east', 'west' and the values must legal and correctly
    # positioned among themselves
    def checkSpatialExt(self, toServer, job_id, ext):
        if ext == None:
            return
        
        if 'north' in ext and 'south' in ext and 'east' in ext and 'west'in ext: # all directions there
            n = ext['north']
            s = ext['south']
            w = ext['west']
            e = ext['east']
            # correctly positioned and have legal value(s)
            if n < s and abs(n) <= 90 and abs(s) <= 90:
                self.handleError(toServer, job_id, 'extents', 'north or south have invalid values', 'ProcessParameterInvalid')
            if w > e and abs(w) <= 180 and abs(e) <= 180:
                self.handleError(toServer, job_id, 'extents', 'east or west have invalid values', 'ProcessParameterInvalid') 
        else:
            self.handleError(toServer, job_id, 'extents','missing extents in extents definition', 'ProcessParameterInvalid')
               
    def compatibleRaster(self,targetRaster, sourceRaster):
        if self.needResample(targetRaster, sourceRaster):
            return self.resample(targetRaster, sourceRaster)
        return sourceRaster
    
    def resample(self,targetRaster, sourceRaster):
        inputRaster = targetRaster.getRaster()
        env = inputRaster.envelope()
        pixSize = targetRaster.getRaster().geoReference().pixelSize()
        projection = 'epsg:' + str(targetRaster['proj'])
        csy = ilwis.CoordinateSystem(projection)
        grf = ilwis.do('createcornersgeoreference', \
                           env.minCorner().x, env.minCorner().y, env.maxCorner().x, env.maxCorner().y, \
                           pixSize, csy, True, '.')
        rm = sourceRaster.getRaster()
        outputRc = ilwis.do("resample", rm, grf, 'nearestneighbour')
        self.createExtra(targetRaster)
        return self.makeOutput([outputRc], self.extra)[0]
    
    def needResample(self,targetRaster, sourceRaster):
        targetRasterIlw = targetRaster.getRaster()     
        sourceRasterIlw = sourceRaster.getRaster() 
        if targetRasterIlw.size() != sourceRasterIlw.size():
            return True
        if not targetRaster.matchProjection(sourceRaster):
            return True
        return False
    
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
        elif  isinstance(a, DataCube):
            t == DTRASTER 
        return t                            

    def getMandatoryParam(self, toServer, job_id, args, key):
        parts = key.split('|')
        result = None
        for p in parts:
            if p in args:
                result = args[p]['resolved']
                if result != None:
                    return result
        self.handleError(toServer, job_id, "parameter", "missing parameter:" + key, 'ProcessParameterInvalid')

    def getOptionalParam(self, toServer, job_id, args, key):
        if key in args:
            return args[key]['resolved']
        return None
            
def setWorkingCatalog(raster, name):
    common.logMessage(logging.INFO, name + ' new working catalog:' + str(raster.dataFolder() ) )
    path = Path(raster.dataFolder()).as_uri()
    if path.find('file://') == -1:
        folderPath = path + "/" + raster['dataFolder']
    else:
        folderPath = path

    ilwis.setWorkingCatalog(folderPath)
    parsed = urlparse(folderPath)
    filepath = unquote(parsed.path.lstrip('/'))
    return filepath    
    
def createOutput(status, value, datatype, format='')        :
    return {"status" : status, "value" : value, "datatype" : datatype, 'format' : format}  

def messageProgress(processOutput, job_id, progress) :
    if processOutput != None:
        processOutput.put({'type': 'progressevent','progress' : progress, 'job_id' : job_id, 'status' : constants.STATUSRUNNING, 'current_operation' : 'dummy'}) 



                







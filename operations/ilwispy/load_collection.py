from openeooperation import *
from operationconstants import *
from constants import constants
from rasterdata import *
from common import getRasterDataSets, saveIdDatabase
import ilwis
from pathlib import Path
from eoreader import *
from eoreader.bands import *
import posixpath
import shutil
import common
from dateutil import parser
from globals import getOperation
from workflow.processGraph import ProcessGraph
import copy


class LoadCollectionOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('load_collection.json')

        self.kind = constants.PDPREDEFINED
        self.bandIdxs = []
        self.lyrIdxs = []

    def unpackOriginalData(self, data, folder):
        common.logMessage(logging.INFO, self.name + ' unpacking original data using eoreader')
        reader = Reader()
        prod = reader.open(data)
        prod.output = folder
        unpackFolderName = "unpacked_" + self.inputRaster['id']
        unpack_folder = os.path.join(folder, unpackFolderName)
      
        oldoutputs = []
        sourceList = {}
        tmp_folder = "tmp_" + prod.condensed_name
        self.clean_tmp(os.path.join(folder, tmp_folder))

        for band in self.inputRaster.getBands():
            bandname = band['commonbandname']
            nn = to_band(bandname)
            prod.load(nn)
            outputs = [f for f in prod.output.glob(tmp_folder+ "*/*.tif")]
            s1 = set(oldoutputs)
            s2 = set(outputs)
            diff = list(s2 - s1)
            oldoutputs = outputs
            if len(diff) == 1:
                sourceList[band['name']] = diff[0].name
        origin = posixpath.dirname(outputs[0])      
        if not os.path.exists(unpack_folder):
            shutil.move(origin, unpack_folder)
        else:
            files = os.listdir(origin)
            for file_name in files:
                src = os.path.join(origin, file_name)
                tar = os.path.join(unpack_folder, file_name)
                shutil.copy(src, tar)
        common.logMessage(logging.INFO, self.name + ' done unpacking original data')
        return sourceList, unpackFolderName

    def clean_tmp(self, tmp_path):
        if os.path.isdir(tmp_path):
            with os.scandir(tmp_path) as entries:
                for entry in entries:
                    if entry.is_file():
                        os.unlink(entry.path)

  
    # checks if a given temporal extent makes sense given the input data
    def checkTemporalExtents(self, toServer, job_id, text):
        if text == None:
            return
        if len(text) != 2:
           self.handleError(toServer, job_id, 'temporal extents','array must have 2 values', 'ProcessParameterInvalid') 
        # convert between a string representation of date-time to a python representation of date-time           
        dt1 = parser.parse(text[0]) 
        dt2 =  parser.parse(text[1])
        if not TEMPORALEXTENT in self.inputRaster:
            self.handleError(toServer, job_id, 'temporal extents','missing extent', 'ProcessParameterInvalid')  
              
        dr1 = parser.parse(self.inputRaster[TEMPORALEXTENT][0])
        dr2 = parser.parse(self.inputRaster[TEMPORALEXTENT][1])
        if dt1 > dt2:
            self.handleError(toServer, job_id, 'temporal extents','invalid extent', 'ProcessParameterInvalid')
        if (dt1 < dr1 and dt2 < dr1) or ( dt1 > dr2 and dt2 > dr2):
            self.handleError(toServer, job_id, 'temporal extents','extents dont overlap', 'ProcessParameterInvalid')
    
    def id2Raster(self, db, id):
        items = db.items()
       
        for item in items:
            if id == item[0] or id == item[1]['title']:
                raster = item[1]
                if not os.path.exists(raster['dataSource']): #virtual datasets with no real source
                    return RasterData(raster)
                return RasterData(raster)

        return None        
    # checks if the given parameters makes senses given the input data. It may also convert data to a more suitable
    # (performant) format if needed.
    def prepare(self, arguments):
        self.runnable = False 
        toServer = None
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id) 
        fileIdDatabase = getRasterDataSets()
        # the requested data could not be found on the server
        rd = self.id2Raster(fileIdDatabase, arguments['id']['resolved'])
        if rd == None:
            self.handleError(toServer, job_id,'input raster not found', 'ProcessParameterInvalid')
        
        self.inputRaster = rd        
        self.dataSource = ''
        oldFolder = folder = self.inputRaster['dataFolder']
        # we don't want 'file' at this stage as it is slow. File can in this case be understood as primary
        # satelite data. unpacking the compressed formats is time consuming. So we transform the original data 
        # a 'metadata' format. Unpack everything and create a .metadata file for this data set. From now only the 
        # .metadata format will be used which has much better performance. 
        if not self.inputRaster.hasData():
            if  not self.inputRaster.sourceIsMetadata():
                self.logProgress(toServer, job_id,"load collection : transforming data", constants.STATUSRUNNING)  
                folder = self.transformOriginalData(fileIdDatabase, folder, oldFolder)                  
            
        common.logMessage(logging.INFO, self.name + ' checking the bands parameter value')   
        if 'bands'in arguments :
            if arguments['bands']['resolved'] != None: #translate band names to indexes as they are easier to work with
                self.bandIdxs = self.inputRaster.getBandIndexes(arguments['bands']['resolved'])
            else: # default band = 0
                self.bandIdxs.append(0)
        else: # default all bands
            self.bandIdxs = self.inputRaster.getBandIndexes([])
        common.logMessage(logging.INFO, self.name + ' checking the temporal extent parameter value')   
        if 'temporal_extent' in arguments:
            # if there is no overlap between temporal extent given and the temporal extent of the actual data
            # an error will thrown as no processing is possible
            self.checkTemporalExtents(toServer, job_id,arguments['temporal_extent']['resolved'])
            self.temporalExtent = arguments['temporal_extent']['resolved']
            #if arguments['temporal_extent']['resolved'] != None:
            self.lyrIdxs = self.inputRaster.getLayerIndexes(arguments['temporal_extent']['resolved'])
            # else:
            #     self.lyrIdxs.append(0)  
        path = setWorkingCatalog(self.inputRaster, self.name)

        common.logMessage(logging.INFO, self.name + ' checking the spatial extent parameter value')  
        if 'spatial_extent' in arguments:
            sect = arguments['spatial_extent']['resolved']
            if sect != None:
                # the parameter spatial_extent is giving in latlon. To see if its values makes sense in
                # the context of the input data its value must be translated to the SRS of the input data.
                # note that in the case of synthetic data self.inputRaster['rasterImplementation'] isn't empty
                # and we can use that data directly
                self.checkSpatialExt(toServer, job_id, sect)
                rband = self.inputRaster.getRaster()
                if not rband:
                    source = self.inputRaster.idx2layer(1)[DATASOURCE]                     
                    datapath = os.path.join(path, source)                            
                    rband = ilwis.RasterCoverage(datapath)
        
                common.registerIlwisIds(job_id, rband) 
                p21 = str(rband.envelope())                   
                csyLL = ilwis.CoordinateSystem("epsg:4326")
                llenv = ilwis.Envelope(ilwis.Coordinate(sect['west'], sect['north']), ilwis.Coordinate(sect['east'], sect['south']))
                envCube = rband.coordinateSystem().convertEnvelope(csyLL, llenv)
                e = str(envCube)
                # if there is no overlap between input data and spatial_extent an error will be thrown as
                # any processing is pointless.
                self.checkOverlap(toServer, job_id,envCube, rband.envelope())
                
                env = e.split(' ')
                if env[0] == '?': # apparently something went wrong with the conversion. Might be an impossible transformation
                    self.handleError(toServer, job_id, 'unusable envelope found ' + str(sect), 'ProcessParameterInvalid')                
                x1 = float(env[0])
                x2 = float(env[2])
                y1 = float(env[1])
                y2 = float(env[3])

                self.inputRaster['spatialExtent'] = [min(x1,x2),min(y1,y2) ,max(x1,x2) ,max(y1,y2) ]
        
        if 'properties' in arguments: # filter properties
            self.properties =  arguments['properties']['resolved']
            
        self.runnable = True
        self.rasterSizesEqual = True
        self.logEndPrepareOperation(job_id)
 
    # unpacks primairy satelite data. creates a metadata file that represents the file and 
    # creates a folder where all the unpacked binary data of the satellite data resides. The orignal
    # data will be moved to a seperate folder and no longer be visible to the system
    def transformOriginalData(self, fileIdDatabase, folder, oldFolder):
        common.logMessage(logging.INFO, self.name + ' unpacking original data to a metadata format: ' + str(self.inputRaster['dataSource']))                 
        self.dataSource = self.inputRaster['dataSource']

        # unpck the original data. EOReader will do this an create a folder where all the data resides
        # basically we use all this but we are going to move and remove some stuff for convenience
        sourceList, unpack_folder = self.unpackOriginalData(self.dataSource, folder)
        for band in self.inputRaster.getBands():
            source = sourceList[band['name']]
            band[DATASOURCE] = source

        folder = os.path.join(folder,unpack_folder)                     
        self.inputRaster['dataFolder'] = unpack_folder
        self.dataSource = folder
        newDataSource = self.inputRaster.toMetadataFile(oldFolder)
        # move the original data to a folder 'original_data'. It is now invisble to the system
        common.logMessage(logging.INFO, self.name + ' move original data to a backup folder: ' + str(self.inputRaster['dataSource']))   
        mvfolder = os.path.join(oldFolder, 'original_data')
        file_name = os.path.basename(self.inputRaster['dataSource'])
        common.makeFolder(mvfolder)
        shutil.move(self.inputRaster['dataSource'], mvfolder + "/" + file_name) 
        #internal databse up to tdata to reflect the new (transformed) data
        self.inputRaster['dataSource'] = newDataSource
        fileIdDatabase[self.inputRaster['id']] = self.inputRaster
        common.logMessage(logging.INFO, self.name + ' update file id database')  
        saveIdDatabase(fileIdDatabase)
        return folder
    
    # the properties parameter is basically a filter on metadata of a dataset. That filter has 
    # the form of a (sub) process graph
    def checkProps(self, openeojob, toServer, fromServer, bandIndexes : list, layer : RasterLayer):
        for prop in self.properties.items():
            for idx in bandIndexes:
                band = self.inputRaster.index2band(idx)
                if prop[0] in band:
                    pgraph = prop[1]
                    copyPg = copy.deepcopy(pgraph)
                    first = next(iter(copyPg))
                    copyPg[first]['arguments'] = {'data' : [band[prop[0]]]}
                    process = ProcessGraph(copyPg, [band[prop[0]]], getOperation)
                    oInfo = process.run(openeojob, toServer, fromServer)  
                    return oInfo['value']
            if prop[0] in layer:
                    pgraph = prop[1]
                    copyPg = copy.deepcopy(pgraph)
                    first = next(iter(copyPg))
                    copyPg[first]['arguments'] = {'data' : [layer[prop[0]]]}
                    process = ProcessGraph(copyPg['process_graph'], [layer[prop[0]]], getOperation)
                    oInfo = process.run(openeojob, toServer, fromServer)  
                    return oInfo['value']
        return True

    ## this method selects the data as defined by the input parameters. Gathers it and , if possible, 
    # creates one or more 3D rasters that are passed through the system. Rasters are per band (if needed)
    # as bands represent different physical properties of a set of layers (t dimension). Each band can have
    # multiple layers which represent the temporal extent. 
    def selectData(self, processOutput,openeojob, bandIndexes, env):
        outputRasters = []
        ilwRasters = []
        
        # synthetic data is already loaded and ready to use. In that case inputRaster['rasterImplementation'] is already there
        # and load_collection doesn't have to do much of actual loading 
            
        if not self.inputRaster.hasData():
            layerTempExtent = []
            implDim =   next(iter(self.inputRaster['implementation']))
            if implDim == DIMTEMPORALLAYER:
                return self.loadByLayer(processOutput, openeojob, bandIndexes, env)
            elif implDim == DIMSPECTRALBANDS:
                return self.loadByBand(processOutput, openeojob, bandIndexes, env)
        
        else:
            inpRasters = self.inputRaster.getRasters()
            rcList = []
            bands = []                     
            for bandIndex in bandIndexes:
                bandIndexList = ''
                layerTempExtent = []

                for lyrIdx in self.lyrIdxs:
                    layer = self.inputRaster.idx2layer(lyrIdx)
                    layerTempExtent.append(layer[TEMPORALEXTENT])
                    #note that the 'true' layerindex is always one smaller than the actual number as the first layer (index 0) is a
                    #generalized layer describing all the layers ; not a real layer. So in the actual binary data this layer doesnt exist
                    #its a metadata thing.
                    if bandIndexList == '':
                        bandIndexList = str(lyrIdx - 1)
                    else:
                        bandIndexList = bandIndexList + ','+ str(lyrIdx - 1)
                bandIndexList = 'rasterbands(' + bandIndexList + ')'                        

                raster = inpRasters[bandIndex]
                if len(self.lyrIdxs) > 0:
                    rc = ilwis.do("selection", raster, "envelope(" + env + ") with: " + bandIndexList)
                else:
                    rc = ilwis.do("selection", raster, "envelope(" + env + ")")  
                rcList.append(rc)
                bands.append(self.inputRaster.index2band(bandIndex))
            extra = { TEMPORALEXTENT : self.temporalExtent, 'bands' : bands, 'epsg' : self.inputRaster['proj'], 'details': {}, 'name' : 'dummy'}                
            if len(layerTempExtent) > 0:
                extra['textsublayers'] = layerTempExtent 
            common.registerIlwisIds(openeojob.job_id, rcList)                              
            rasterData = RasterData()
            rasterData.load(rcList, 'ilwisraster', extra )
            outputRasters.append(rasterData) 

        return outputRasters 

    def loadByBand(self, processOutput, openeojob, bandIndexes, env):
        ilwRasters = []
        outputRasters = []
        layerTempExtent = []
        bands = []
        ev = ilwis.Envelope("(" + env + ")")
        common.logMessage(logging.INFO, self.name + ' select layers from appropriate bands')                   
        for lyrIdx in self.lyrIdxs:
            layer = self.inputRaster.idx2layer(lyrIdx)
            if layer != None:
                valueOk = False 
                hasProp = hasattr(self, 'properties')
                
                # if we have a property filter we must check if this raster satisfies the condition(s)
                if hasProp:
                    valueOk = self.checkProps(openeojob, processOutput,None, bandIndexes, layer)
                if not hasProp or valueOk:
                    layerTempExtent.append(layer[TEMPORALEXTENT])
                    for bandIdx in bandIndexes:
                        band = self.inputRaster.index2band(bandIdx)
                        ilwBand = ilwis.RasterCoverage(band[DATASOURCE])  
                        if ilwBand.size() == ilwis.Size(0,0,0):
                            self.handleError(processOutput, openeojob.job_id, 'Input raster', 'invalid sub-band:' + layer[DATASOURCE], 'ProcessParameterInvalid')
                        common.registerIlwisIds(ilwBand)
                        nodata = self.inputRaster['nodata']
                        undefRepl = ""
                        if nodata != constants.RUNDEFFL or nodata != constants.RUNDEFI32:
                            undefRepl = " pixelvalue!=" + str(nodata)
                        ss = str(ilwBand.envelope())
                        if not ev.equalsP(ilwBand.envelope(), 0.001, 0.001, 0.001):
                            rc = ilwis.do("selection", ilwBand, undefRepl + " with: envelope(" + env + ")") 
                        else:
                            rc = ilwis.do("selection", ilwBand, undefRepl )
                        common.registerIlwisIds(openeojob.job_id, rc)  
                        ilwRasters.append(rc) 
                        bands.append(band)

            extra = { TEMPORALEXTENT : self.temporalExtent, 'bands' : bands, 'epsg' : self.inputRaster['proj'], 'details': {}, 'name' : 'dummy'}                
            if len(layerTempExtent) > 0:
                extra['textsublayers'] = layerTempExtent 
            common.registerIlwisIds(openeojob.job_id, ilwRasters)                              
            rasterData = RasterData()
            rasterData.load(ilwRasters, 'ilwisraster', extra )
            outputRasters.append(rasterData)   

        return outputRasters
    
    def loadByLayer(self, processOutput, openeojob, bandIndexes, env):
        layerTempExtent = []
        loadedRasters = []
        outputRasters = []
        common.logMessage(logging.INFO, self.name + ' load data by layer')  
        for lyrIdx in self.lyrIdxs:
            layer = self.inputRaster.idx2layer(lyrIdx)
            if layer != None:
                valueOk = False 
                hasProp = hasattr(self, 'properties')
                
                # if we have a property filter we must check if this raster satisfies the condition(s)
                if hasProp:
                    valueOk = self.checkProps(openeojob, processOutput,None, bandIndexes, layer)
                    ## only add layers that either don't have the property or if they have if must match the condition                        
                if not hasProp or valueOk:
                    layerTempExtent.append(layer[TEMPORALEXTENT])
                    ilwLayer = ilwis.RasterCoverage(layer[DATASOURCE])
                    if ilwLayer.size() == ilwis.Size(0,0,0):
                        self.handleError(processOutput, openeojob.job_id, 'Input raster', 'invalid sub-band:' + layer[DATASOURCE], 'ProcessParameterInvalid')
                    common.registerIlwisIds(openeojob.job_id, ilwLayer)
                    loadedRasters.append(ilwLayer) 
            
        newRasters = self.selectBandsFromLayers(bandIndexes, env, loadedRasters)
        
        extra = self.constructExtraParams(self.inputRaster, self.temporalExtent, bandIndexes)
        extra['textsublayers'] = layerTempExtent
        outputRasters.append(self.createOutput(0, newRasters, extra))                  

        return outputRasters

    def selectBandsFromLayers(self, bandIndexes, env, loadedRasters):
        ev = ilwis.Envelope("(" + env + ")")
        ilwRasters = [] 
        outRasters = [] 
        common.logMessage(logging.INFO, self.name + ' select bands from appropriate layers: ')                    
        for bandIndex in bandIndexes:
            bandIndexList = 'rasterbands(' + str(bandIndex) + ')'

            for layer in loadedRasters:   
                    # if the requested enevelope doesn't match the envelope of the inputdata we execute the 'select'
                    # operation to get a portion of the raster that we need. we also include the exclusion of (if defined)                         
                    # nodata values. As rasters are by default filled with undefs this will convert nodata to ilwis undefs
                    # which is its equivalent
                nodata = self.inputRaster['nodata']
                undefRepl = ""
                if nodata != constants.RUNDEFFL or nodata != constants.RUNDEFI32:
                    undefRepl = " pixelvalue!=" + str(nodata) + " and "
                if not ev.equalsP(loadedRasters[0].envelope(), 0.001, 0.001, 0.001):
                    rc = ilwis.do("selection", layer, undefRepl + "with: envelope(" + env + ") and " + bandIndexList) 
                else:
                    rc = ilwis.do("selection", layer, undefRepl + "with: " + bandIndexList)                                                           
                ilwRasters.append(rc)
            newRaster = self.collectRasters(ilwRasters)
            outRasters.append(newRaster)
                        
        return outRasters

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob, self.inputRaster['title'])
            
            indexes = str(self.bandIdxs).lstrip('[').rstrip(']')
            indexes = [int(ele) for ele in indexes.split(',')]
            ext = self.inputRaster['spatialExtent']
            #env = str(ext[0]) + " "+ str(ext[2]) + "," + str(ext[1]) + " " +str(ext[3])
            env = str(ext[0]) + " "+ str(ext[1]) + "," + str(ext[2]) + " " +str(ext[3])

            outputRasters = self.selectData(processOutput,openeojob, indexes, env)

            self.logEndOperation(processOutput,openeojob, outputs=outputRasters, extraMessage=self.inputRaster['title'])
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)
        
        message = common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', message, constants.DTERROR)
           
def registerOperation():
     return LoadCollectionOperation()
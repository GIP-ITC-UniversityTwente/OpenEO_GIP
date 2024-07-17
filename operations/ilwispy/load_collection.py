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
        #os.mkdir(folder)
        reader = Reader()
        prod = reader.open(data)
        prod.output = folder
        unpackFolderName = "unpacked_" + self.inputRaster.id
        unpack_folder = os.path.join(folder, unpackFolderName)
      
        oldoutputs = []
        sourceList = {}
        for band in self.inputRaster.bands:
            bandname = band['commonbandname']
            nn = to_band(bandname)
            prod.load(nn)
            outputs = [f for f in prod.output.glob("tmp*/*.tif")]
            s1 = set(oldoutputs)
            s2 = set(outputs)
            diff = list(s2 - s1)
            oldoutputs = outputs
            if len(diff) == 1:
                sourceList[band['name']] = diff[0].name
              

        
        os.rename(posixpath.dirname(outputs[0]), unpack_folder)
        return sourceList, unpackFolderName

  
    # checks if a given temporal extent makes sense given the input data
    def checkTemporalExtents(self, toServer, job_id, text):
        if text == None:
            return
        if len(text) != 2:
           self.handleError(toServer, job_id, 'temporal extents','array must have 2 values', 'ProcessParameterInvalid') 
        # convert between a string representation of date-time to a python representation of date-time           
        dt1 = parser.parse(text[0]) 
        dt2 =  parser.parse(text[1])
        dr1 = parser.parse(self.inputRaster['temporalExtent'][0])
        dr2 = parser.parse(self.inputRaster['temporalExtent'][1])
        if dt1 > dt2:
            self.handleError(toServer, job_id, 'temporal extents','invalid extent', 'ProcessParameterInvalid')
        if (dt1 < dr1 and dt2 < dr1) or ( dt1 > dr2 and dt2 > dr2):
            self.handleError(toServer, job_id, 'temporal extents','extents dont overlap', 'ProcessParameterInvalid')

    # checks if the given parameters makes senses given the input data. It may also convert data to a more suitable
    # (performant) format if needed.
    def prepare(self, arguments):
        self.runnable = False 
        toServer = None
        job_id = None
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']

        fileIdDatabase = getRasterDataSets()          
        self.inputRaster = fileIdDatabase[arguments['id']['resolved']]
        # the requested data could not be found on the server
        if self.inputRaster == None:
            self.handleError(toServer, job_id,'input raster not found', 'ProcessParameterInvalid')
        
        self.dataSource = ''
        oldFolder = folder = self.inputRaster['dataFolder']
        # we don't want 'file' at this stage as it is slow. File can in this case be understood as primary
        # satelite data. unpacking the compressed formats is time consuming. So we transform the original data 
        # a 'metadata' format. Unpack everything and create a .metadata file for this data set. From now only the 
        # .metadata format will be used which has much better performance. 
        if  self.inputRaster['type'] == 'file': 
            self.logProgress(toServer, job_id,"load collection : transforming data", constants.STATUSRUNNING)                   
            folder = self.transformOriginalData(fileIdDatabase, folder, oldFolder)                  
            
        
        if 'bands'in arguments :
            if arguments['bands']['resolved'] != None: #translate band names to indexes as they are easier to work with
                self.bandIdxs = self.inputRaster.getBandIndexes(arguments['bands']['resolved'])
            else: # default band = 0
                self.bandIdxs.append(0)
        else: # default all bands
            self.bandIdxs = self.inputRaster.getBandIndexes([])

        if 'temporal_extent' in arguments:
            # if there is no overlap between temporal extent given and the temporal extent of the actual data
            # an error will thrown as no processing is possible
            self.checkTemporalExtents(toServer, job_id,arguments['temporal_extent']['resolved'])
            self.temporalExtent = arguments['temporal_extent']['resolved']
            #if arguments['temporal_extent']['resolved'] != None:
            self.lyrIdxs = self.inputRaster.getLayerIndexes(arguments['temporal_extent']['resolved'])
            # else:
            #     self.lyrIdxs.append(0)  
        path = Path(folder).as_uri()
        ilwis.setWorkingCatalog(path)

        if 'spatial_extent' in arguments:
            sect = arguments['spatial_extent']['resolved']
            if sect != None:
                # the parameter spatial_extent is giving in latlon. To see if its values makes sense in
                # the context of the input data its value must be translated to the SRS of the input data.
                # note that in the case of synthetic data self.inputRaster['rasterImplementation'] isn't empty
                # and we can use that data directly
                self.checkSpatialExt(toServer, job_id, sect)
                if len(self.inputRaster[DATAIMPLEMENTATION]) == 0:
                    source = self.inputRaster.idx2layer(1)['source']                     
                    datapath = os.path.join(path, source)                            
                    rband = ilwis.RasterCoverage(datapath)
                else:
                    key = next(iter(self.inputRaster[DATAIMPLEMENTATION]))
                    rband = self.inputRaster[DATAIMPLEMENTATION][key]

                common.registerIlwisIds(rband)                    
                csyLL = ilwis.CoordinateSystem("epsg:4326")
                llenv = ilwis.Envelope(ilwis.Coordinate(sect['west'], sect['south']), ilwis.Coordinate(sect['east'], sect['north']))
                envCube = rband.coordinateSystem().convertEnvelope(csyLL, llenv)
                e = str(envCube)
                # if there is no overlap between input data and spatial_extent an error will be thrown as
                # any processing is pointless.
                self.checkOverlap(toServer, job_id,envCube, rband.envelope())
                
                env = e.split(' ')
                if env[0] == '?': # apparently something went wrong with the conversion. Might be an impossible transformation
                    self.handleError(toServer, job_id, 'unusable envelope found ' + str(sect), 'ProcessParameterInvalid')

                self.inputRaster['spatialExtent'] = [env[0], env[2], env[1], env[3]]
        
        if 'properties' in arguments: # filter properties
            self.properties =  arguments['properties']['resolved']
            
        self.runnable = True
        self.rasterSizesEqual = True
 
    # unpacks primairy satelite data. creates a metadata file that represents the file and 
    # creates a folder where all the unpacked binary data of the satellite data resides. The orignal
    # data will be moved to a seperate folder and no longer be visible to the system
    def transformOriginalData(self, fileIdDatabase, folder, oldFolder):
        self.dataSource = self.inputRaster['dataSource']

        # unpck the original data. EOReader will do this an create a folder where all the data resides
        # basically we use all this but we are going to move and remove some stuff for convenience
        sourceList, unpack_folder = self.unpackOriginalData(self.dataSource, folder)
        for band in self.inputRaster.getBands().items():
            source = sourceList[band[1]['name']]
            band['source'] = source

        folder = os.path.join(folder,unpack_folder)                     
        self.inputRaster['dataFolder'] = folder
        self.dataSource = folder
        newDataSource = self.inputRaster.toMetadataFile(oldFolder)
        # move the original data to a folder 'original_data'. It is now invisble to the system
        mvfolder = os.path.join(oldFolder, 'original_data')
        file_name = os.path.basename(self.inputRaster['dataSource'])
        common.makeFolder(mvfolder)
        shutil.move(self.inputRaster['dataSource'], mvfolder + "/" + file_name) 
        #internal databse up to tdata to reflect the new (transformed) data
        self.inputRaster['dataSource'] = newDataSource
        fileIdDatabase[self.inputRaster['id']] = self.inputRaster
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
        ev = ilwis.Envelope("(" + env + ")")
        # synthetic data is already loaded and ready to use. In that case inputRaster['rasterImplementation'] is already there
        # and load_collection doesn't have to do much of actual loading             
        if len(self.inputRaster[DATAIMPLEMENTATION]) == 0:
            layerTempExtent = []
            loadedRasters = []
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
                        layerTempExtent.append(layer['temporalExtent'])
                        ilwLayer = ilwis.RasterCoverage(layer['source'])
                        if ilwLayer.size() == ilwis.Size(0,0,0):
                            self.handleError(processOutput, openeojob.job_id, 'Input raster', 'invalid sub-band:' + layer['source'], 'ProcessParameterInvalid')
                        loadedRasters.append(ilwLayer) 
                                                        
                  
            
            for bandIndex in bandIndexes:
                bandIndexList = 'rasterbands(' + str(bandIndex) + ')'
                layers = []                                
                for layer in loadedRasters:   
                    # if the requested enevelope doesn't match the envelope of the inputdata we execute the 'select'
                    # operation to get a portion of the raster that we need                         
                    nodata = self.inputRaster['nodata']
                    undefRepl = ""
                    if nodata != constants.RUNDEFFL or nodata != constants.RUNDEFI32:
                        undefRepl = " pixelvalue!=" + str(nodata) + " and "
                    if not ev.equalsP(loadedRasters[0].envelope(), 0.001, 0.001, 0.001):
                        rc = ilwis.do("selection", layer, undefRepl + "with: envelope(" + env + ") and " + bandIndexList) 
                    else:
                        rc = ilwis.do("selection", layer, undefRepl + "with: " + bandIndexList)                                                           
                    layers.append(rc)
                    common.registerIlwisIds(layers)
                newBand = self.collectRasters(layers)
                sz = newBand.size()
                sz = str(sz)
                ilwRasters.append(newBand)
            extra = self.constructExtraParams(self.inputRaster, self.temporalExtent, bandIndexes)
            extra['textsublayers'] = layerTempExtent
            keys = []
            for i in bandIndexes:
                keys.append(str(i))
            extra['rasterkeys'] = keys
            outputRasters.append(self.createOutput(0, ilwRasters, extra))
        else:
            it = iter(self.inputRaster[DATAIMPLEMENTATION])
            it2= iter(self.inputRaster[METADATDEFDIM][DIMSPECTRALBANDS]['items'])   
            rcList = []
            bands = []                     
            for bandIndex in bandIndexes:
                bandIndexList = ''
                layerTempExtent = []

                for lyrIdx in self.lyrIdxs:
                    layer = self.inputRaster.idx2layer(lyrIdx)
                    layerTempExtent.append(layer['temporalExtent'])
                    if bandIndexList == '':
                        bandIndexList = str(lyrIdx)
                    else:
                        bandIndexList = bandIndexList + ','+ str(lyrIdx)
                bandIndexList = 'rasterbands(' + bandIndexList + ')'                        
                key = next(it)
                key2 = next(it2) 
                raster = self.inputRaster[DATAIMPLEMENTATION][key]  
                if len(self.lyrIdxs) > 0:
                    rc = ilwis.do("selection", raster, "envelope(" + env + ") with: " + bandIndexList)
                else:
                    rc = ilwis.do("selection", raster, "envelope(" + env + ")")  
                rcList.append(rc)
                bands.append(self.inputRaster[METADATDEFDIM][DIMSPECTRALBANDS]['items'][key2])
            extra = { 'temporalExtent' : self.temporalExtent, 'bands' : bands, 'epsg' : self.inputRaster['proj:epsg'], 'details': {}, 'name' : 'dummy'}                
            if len(layerTempExtent) > 0:
                extra['textsublayers'] = layerTempExtent 
            common.registerIlwisIds(rcList)                              
            rasterData = RasterData()
            rasterData.load(rcList, 'ilwisraster', extra )
            outputRasters.append(rasterData) 

        return outputRasters                   


    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob, self.inputRaster['title'])
            
            indexes = str(self.bandIdxs).lstrip('[').rstrip(']')
            indexes = [int(ele) for ele in indexes.split(',')]
            ext = self.inputRaster['spatialExtent']
            env = str(ext[0]) + " " + str(ext[2]) + "," + str(ext[1]) + " " +str(ext[3])

            outputRasters = self.selectData(processOutput,openeojob, indexes, env)

            self.logEndOperation(processOutput,openeojob, outputs=outputRasters, extraMessage=self.inputRaster['title'])
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)
        
        message = common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', message, constants.DTERROR)
           
def registerOperation():
     return LoadCollectionOperation()
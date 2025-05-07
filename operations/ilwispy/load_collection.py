from openeooperation import *
#from operationconstants import *
from constants import constants
from datacube import *
from common import openeoip_config, saveIdDatabase
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
from multiprocessing import Lock
import tests.addTestRasters as tr

# gets all rasterdata sets that are registered in the system
# this is basically a cached value for performance reasons and consitency
def getRasterDataSets(includeSynteticData=True):
    home = Path.home()
    loc = openeoip_config['data_locations']['system_files']
    sytemFolder = os.path.join(home, loc['location'])        
    propertiesFolder = os.path.join(home, sytemFolder)
    raster_data_sets = dict()
    if ( os.path.exists(propertiesFolder)):
        propertiesPath = os.path.join(propertiesFolder, 'id2filename.table')
        if ( os.path.exists(propertiesPath)):
            lock = Lock()
            lock.acquire()
            with open(propertiesPath, 'r') as f:
                data = f.read()
            f.close()
            lock.release()    
            raster_data_sets =  json.loads(data)

    if includeSynteticData:      
        rasters = tr.setTestRasters(5)
        for r in rasters:
            raster_data_sets[r['id']] = r         
    return raster_data_sets

class LoadCollectionOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('load_collection.json')

        self.kind = constants.PDPREDEFINED
        self.bandIdxs = []
        self.lyrIdxs = []

    def unpackOriginalData(self, data, folder):

        common.logMessage(logging.INFO, self.name + ' unpacking original data using eoreader')
        reader = Reader()
        product = reader.open(data)
        product.output = folder

        unpack_folder_name = f"unpacked_{self.inputRaster['id']}"
        unpack_folder = os.path.join(folder, unpack_folder_name)

        tmp_folder = f"tmp_{product.condensed_name}"
        self._cleanTemporaryFolder(os.path.join(folder, tmp_folder))

        source_list = self._processBands(product, tmp_folder)
        self._moveUnpackedData(product, unpack_folder, tmp_folder)

        common.logMessage(logging.INFO, self.name + ' done unpacking original data')
        return source_list, unpack_folder_name

    def _cleanTemporaryFolder(self, tmp_path):
        """
        Cleans up the temporary folder by removing all files.

        Args:
            tmp_path: The path to the temporary folder.
        """
        if os.path.isdir(tmp_path):
            with os.scandir(tmp_path) as entries:
                for entry in entries:
                    if entry.is_file():
                        os.unlink(entry.path)

    def _processBands(self, product, tmp_folder):
        """
        Processes the bands of the product and maps them to their file paths.

        Args:
            product: The EOReader product object.
            tmp_folder: The temporary folder where the band files are stored.

        Returns:
            A dictionary mapping band names to their file paths.
        """
        source_list = {}
        old_outputs = []

        for band in self.inputRaster.getBands():
            band_name = band['commonbandname']
            band_enum = to_band(band_name)
            product.load(band_enum)

            outputs = [f for f in product.output.glob(f"{tmp_folder}*/*.tif")]
            new_files = self._getNewFiles(old_outputs, outputs)
            old_outputs = outputs

            if len(new_files) == 1:
                source_list[band['name']] = new_files[0].name

        return source_list

    def _getNewFiles(self, old_outputs, new_outputs):
        """
        Identifies new files generated during the band processing.

        Args:
            old_outputs: The list of previously generated files.
            new_outputs: The list of newly generated files.

        Returns:
            A list of new files.
        """
        old_set = set(old_outputs)
        new_set = set(new_outputs)
        return list(new_set - old_set)

    def _moveUnpackedData(self, product, unpack_folder, tmp_folder):
        """
        Moves the unpacked data to the specified folder.

        Args:
            product: The EOReader product object.
            unpack_folder: The folder where the unpacked data will be moved.
        """
        if not os.path.exists(tmp_folder): 
            return
        dirfiles = tmp_folder + "*/*.tif"
        files = product.output.glob(dirfiles)
       # if len(files) == 0:
       #     return
        origin = posixpath.dirname(files[0])

        if not os.path.exists(unpack_folder):
            shutil.move(origin, unpack_folder)
        else:
            files = os.listdir(origin)
            for file_name in files:
                src = os.path.join(origin, file_name)
                dest = os.path.join(unpack_folder, file_name)
                shutil.copy(src, dest)

    
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
                    return DataCube(raster)
                return DataCube(raster)

        return None 
    def prepare(self, arguments):
            """
            Prepares the operation by validating and transforming input data.
        
            Args:
                arguments: The input arguments for the operation.
            """
            self.runnable = False
            to_server, job_id = self._initializePreparation(arguments)
            file_id_database = getRasterDataSets()
        
            # Validate and load the input raster
            self.inputRaster = self._loadInputRaster(file_id_database, arguments, to_server, job_id)
        
            # Transform the original data if necessary
            self._transformDataIfNeeded(file_id_database, to_server, job_id)
        
            # Process bands
            self._processBandsArgument(arguments)
        
            # Process temporal extent
            self._processTemporalExtent(arguments, to_server, job_id)
            path = setWorkingCatalog(self.inputRaster, self.name)
            # Process spatial extent
            self._processSpatialExtent(arguments, to_server, job_id, path)
        
            # Process properties
            self._processProperties(arguments)
        
            self.runnable = True
            self.rasterSizesEqual = True
            self.logEndPrepareOperation(job_id)
        
        # Helper Functions
        
    def _initializePreparation(self, arguments):
            """
            Initializes the preparation process by extracting default arguments.
        
            Args:
                arguments: The input arguments for the operation.
        
            Returns:
                A tuple containing the server object and job ID.
            """
            to_server, job_id = self.getDefaultArgs(arguments)
            self.logStartPrepareOperation(job_id)
            return to_server, job_id
        
    def _loadInputRaster(self, file_id_database, arguments, to_server, job_id):
            """
            Loads the input raster based on the provided arguments.
        
            Args:
                file_id_database: The database of raster datasets.
                arguments: The input arguments for the operation.
                to_server: The server object for communication.
                job_id: The job ID for logging.
        
            Returns:
                The loaded raster data.
            """
            raster_data = self.id2Raster(file_id_database, arguments['id']['resolved'])
            if raster_data['proj'] == '0':
                raster_data['proj'] = raster_data.getRaster().coordinateSystem().toProj4();
      
            if raster_data is None:
                self.handleError(to_server, job_id, 'input raster not found', 'ProcessParameterInvalid')
            return raster_data
        
    def _transformDataIfNeeded(self, file_id_database, to_server, job_id):
            """
            Transforms the original data to a metadata format if necessary.
        
            Args:
                file_id_database: The database of raster datasets.
                to_server: The server object for communication.
                job_id: The job ID for logging.
        
            Returns:
                The folder containing the transformed data.
            """
            folder = self.inputRaster['dataFolder']
            if not self.inputRaster.hasData() and not self.inputRaster.sourceIsMetadata():
                self.logProgress(to_server, job_id, "load collection : transforming data", constants.STATUSRUNNING)
                folder = self.transformOriginalData(file_id_database, folder, folder)
            return folder
        
    def _processBandsArgument(self, arguments):
            """
            Processes the 'bands' argument and translates band names to indexes.
        
            Args:
                arguments: The input arguments for the operation.
            """
         
            common.logMessage(logging.INFO, self.name + ' checking the bands parameter value')
            if 'bands'in arguments :
                if arguments['bands']['resolved'] != None: #translate band names to indexes as they are easier to work with
                    self.bandIdxs = self.inputRaster.getBandIndexes(arguments['bands']['resolved'])
                else: # default band = 0
                    self.bandIdxs.append(0)
            else: # default all bands
                self.bandIdxs = self.inputRaster.getBandIndexes([])            

        
    def _processTemporalExtent(self, arguments, to_server, job_id):
            """
            Processes the 'temporal_extent' argument and validates it.
        
            Args:
                arguments: The input arguments for the operation.
                to_server: The server object for communication.
                job_id: The job ID for logging.
            """
            common.logMessage(logging.INFO, self.name + ' checking the temporal extent parameter value')
            if 'temporal_extent' in arguments:
                self.checkTemporalExtents(to_server, job_id, arguments['temporal_extent']['resolved'])
                self.temporalExtent = arguments['temporal_extent']['resolved']
                self.lyrIdxs = self.inputRaster.getLayerIndexes(arguments['temporal_extent']['resolved'])
        
    def _processSpatialExtent(self, arguments, to_server, job_id, folder):
            """
            Processes the 'spatial_extent' argument and validates it.
        
            Args:
                arguments: The input arguments for the operation.
                to_server: The server object for communication.
                job_id: The job ID for logging.
                folder: The folder containing the input raster data.
            """
            common.logMessage(logging.INFO, self.name + ' checking the spatial extent parameter value')
            if 'spatial_extent' in arguments:
                spatial_extent = arguments['spatial_extent']['resolved']
                if spatial_extent is not None:
                    self.checkSpatialExt(to_server, job_id, spatial_extent)
                    rband = self.inputRaster.getRaster()
                    if not rband:
                        source = self.inputRaster.idx2layer(1)[DATASOURCE]
                        datapath = os.path.join(folder, source)
                        rband = ilwis.RasterCoverage(datapath)
        
                    common.registerIlwisIds(job_id, rband)
                    env_cube = self._convertSpatialExtent(spatial_extent, rband)
                    self._validateSpatialExtent(to_server, job_id, env_cube, rband.envelope())
                    self.inputRaster['spatialExtent'] = self._extractSpatialExtent(env_cube)
        
    def _convertSpatialExtent(self, spatial_extent, rband):
            """
            Converts the spatial extent from lat/lon to the raster's coordinate system.
        
            Args:
                spatial_extent: The spatial extent in lat/lon.
                rband: The raster band object.
        
            Returns:
                The converted spatial extent.
            """
            csy_ll = ilwis.CoordinateSystem("epsg:4326")
            llenv = ilwis.Envelope(
                ilwis.Coordinate(spatial_extent['west'], spatial_extent['north']),
                ilwis.Coordinate(spatial_extent['east'], spatial_extent['south'])
            )
            return rband.coordinateSystem().convertEnvelope(csy_ll, llenv)
        
    def _validateSpatialExtent(self, to_server, job_id, env_cube, raster_envelope):
            """
            Validates the spatial extent to ensure it overlaps with the raster's extent.
        
            Args:
                to_server: The server object for communication.
                job_id: The job ID for logging.
                env_cube: The converted spatial extent.
                raster_envelope: The raster's envelope.
            """
            self.checkOverlap(to_server, job_id, env_cube, raster_envelope)
            env = str(env_cube).split(' ')
            if env[0] == '?':
                self.handleError(to_server, job_id, 'unusable envelope found', 'ProcessParameterInvalid')
        
    def _extractSpatialExtent(self, env_cube):
            """
            Extracts the spatial extent from the converted envelope.
        
            Args:
                env_cube: The converted spatial extent.
        
            Returns:
                A list representing the spatial extent.
            """
            env = str(env_cube).split(' ')
            x1, x2 = float(env[0]), float(env[2])
            y1, y2 = float(env[1]), float(env[3])
            return [min(x1, x2), min(y1, y2), max(x1, x2), max(y1, y2)]
        
    def _processProperties(self, arguments):
            """
            Processes the 'properties' argument.
        
            Args:
                arguments: The input arguments for the operation.
            """
            if 'properties' in arguments:
                self.properties = arguments['properties']['resolved']       
  
 
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
    
    def selectData(self, process_output, openeo_job, band_indexes, spatial_env):
        """
        Selects the data as defined by the input parameters. Gathers it and, if possible,
        creates one or more 3D rasters that are passed through the system.

        Args:
            process_output: The output of the process.
            openeo_job: The OpenEO job object.
            band_indexes: List of band indexes to select.
            spatial_env: The spatial envelope as a string.

        Returns:
            A list of output rasters.
        """
        if not self.inputRaster.hasData():
            return self._handleSyntheticData(process_output, openeo_job, band_indexes, spatial_env)
        else:
            return self._handleRealData(process_output, openeo_job, band_indexes, spatial_env)

    def _handleSyntheticData(self, process_output, openeo_job, band_indexes, spatial_env):
        """
        Handles the case where synthetic data is already loaded and ready to use.

        Args:
            process_output: The output of the process.
            openeo_job: The OpenEO job object.
            band_indexes: List of band indexes to select.
            spatial_env: The spatial envelope as a string.

        Returns:
            A list of output rasters.
        """
        implementation_dimension = next(iter(self.inputRaster['implementation']))
        if implementation_dimension == DIMTEMPORALLAYER:
            return self.loadByLayer(process_output, openeo_job, band_indexes, spatial_env)
        elif implementation_dimension == DIMSPECTRALBANDS:
            return self.loadByBand(process_output, openeo_job, band_indexes, spatial_env)

    def _handleRealData(self, process_output, openeo_job, band_indexes, spatial_env):
        """
        Handles the case where real data needs to be processed.

        Args:
            process_output: The output of the process.
            openeo_job: The OpenEO job object.
            band_indexes: List of band indexes to select.
            spatial_env: The spatial envelope as a string.

        Returns:
            A list of output rasters.
        """
        input_rasters = self.inputRaster.getRasters()
        raster_coverage_list = []
        selected_bands = []
        layer_temporal_extent = []

        for band_index in band_indexes:
            band_index_list = self._getBandIndexList(band_index, layer_temporal_extent)
            raster_coverage = self._selectRasterCoverage(input_rasters, band_index, band_index_list, spatial_env)
            raster_coverage_list.append(raster_coverage)
            selected_bands.append(self.inputRaster.index2band(band_index))

        return self._createOutputRasters(openeo_job, raster_coverage_list, selected_bands, layer_temporal_extent)

    def _getBandIndexList(self, band_index, layer_temporal_extent):
        """
        Constructs the band index list for a given band index.

        Args:
            band_index: The band index to process.
            layer_temporal_extent: A list to store the temporal extent of layers.

        Returns:
            A string representing the band index list.
        """
        band_index_list = ''
        for layer_index in self.lyrIdxs:
            layer = self.inputRaster.idx2layer(layer_index)
            layer_temporal_extent.append(layer[TEMPORALEXTENT])
            if band_index_list == '':
                band_index_list = str(layer_index - 1)
            else:
                band_index_list += ',' + str(layer_index - 1)
        return f'rasterbands({band_index_list})'

    def _selectRasterCoverage(self, input_rasters, band_index, band_index_list, spatial_env):
        """
        Selects the raster coverage for a given band index.

        Args:
            input_rasters: The list of input rasters.
            band_index: The band index to process.
            band_index_list: The band index list as a string.
            spatial_env: The spatial envelope as a string.

        Returns:
            The selected raster coverage.
        """
        raster = input_rasters[band_index]
        if len(self.lyrIdxs) > 0:
            return ilwis.do("selection", raster, f"envelope({spatial_env}) with: {band_index_list}")
        else:
            return ilwis.do("selection", raster, f"envelope({spatial_env})")

    def _createOutputRasters(self, openeo_job, raster_coverage_list, selected_bands, layer_temporal_extent):
        """
        Creates the output rasters based on the selected data.

        Args:
            openeo_job: The OpenEO job object.
            raster_coverage_list: The list of raster coverages.
            selected_bands: The list of selected bands.
            layer_temporal_extent: The temporal extent of layers.

        Returns:
            A list of output rasters.
        """
        extra_metadata = {
            TEMPORALEXTENT: self.temporalExtent,
            'bands': selected_bands,
            'epsg': self.inputRaster['proj'],
            'details': {},
            'name': 'dummy'
        }
        if len(layer_temporal_extent) > 0:
            extra_metadata['textsublayers'] = layer_temporal_extent

        common.registerIlwisIds(openeo_job.job_id, raster_coverage_list)
        raster_data = DataCube()
        raster_data.load(raster_coverage_list, 'ilwisraster', extra_metadata)
        return [raster_data]

    def loadByBand(self, processOutput, openeojob, bandIndexes, env):
        common.logMessage(logging.INFO, self.name + ' load data by band')
        loadedRasters = self.loadRastersByBand(bandIndexes)
        selectedRasters = self.selectLayersFromBands(env, loadedRasters)
        extra = self.constructExtraParams(self.inputRaster, self.temporalExtent, bandIndexes)
        outputRasters = [self.createOutput(0, selectedRasters, extra)]
        return outputRasters

    def loadRastersByBand(self, bandIndexes):
        loadedRasters = []
        for bandIdx in bandIndexes:
            band = self.inputRaster.index2band(bandIdx)
            ilwBand = ilwis.RasterCoverage(band[DATASOURCE])
            if ilwBand.size() == ilwis.Size(0, 0, 0):
                self.handleError(None, None, 'Input raster', 'invalid sub-band:' + band[DATASOURCE], 'ProcessParameterInvalid')
            common.registerIlwisIds(ilwBand)
            loadedRasters.append(ilwBand)
        return loadedRasters

    def selectLayersFromBands(self, env, loadedRasters):
        ev = ilwis.Envelope("(" + env + ")")
        ilwRasters = []
        for layer in loadedRasters:
            nodata = self.inputRaster['nodata']
            undefRepl = ""
            if nodata != constants.RUNDEFFL or nodata != constants.RUNDEFI32:
                undefRepl = " pixelvalue!=" + str(nodata)
            if not ev.equalsP(layer.envelope(), 0.001, 0.001, 0.001):
                rc = ilwis.do("selection", layer, undefRepl + " with: envelope(" + env + ")")
            else:
                rc = ilwis.do("selection", layer, undefRepl)
            ilwRasters.append(rc)
        return self.collectRasters(ilwRasters)
    
    
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
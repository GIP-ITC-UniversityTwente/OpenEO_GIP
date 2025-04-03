from openeooperation import *
from operationconstants import *
from constants import constants
from common import openeoip_config
from rasterdata import RasterData, matchBands, matchesTemporalExtent

class MaskOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('mask.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        """
        Prepares the MaskOperation by validating input arguments and setting up the operation.

        Args:
            arguments: The input arguments for the operation.
        """
        self.runnable = False
        toServer, job_id = self._initializePreparation(arguments)

        dataRasters = arguments['data']['resolved']
        maskRasters = arguments['mask']['resolved']

        if not overlaps(dataRasters[0]['boundingbox'], maskRasters[0]['boundingbox']):
            self.handleError(toServer, job_id, 'input rasters', 'data and mask don\'t overlap', 'ProcessParameterInvalid')

        self.replacement = arguments['replacement']['resolved']
        self.singleMask = len(maskRasters) == 1

        self.rasters = self._processRasters(toServer, job_id, dataRasters, maskRasters)
        self._finalizePreparation(job_id)

    def _processRasters(self, toServer, job_id, dataRasters, maskRasters):
        """
        Processes the data and mask rasters, validating and resampling as needed.

        Args:
            toServer: The server object for communication.
            job_id: The job ID for logging.
            dataRasters: The list of data rasters.
            maskRasters: The list of mask rasters.

        Returns:
            A list of dictionaries containing data and mask rasters with resampling information.
        """
        rasters = []

        if not self.singleMask:
            if len(dataRasters) != len(maskRasters):
                self.handleError(toServer, job_id, 'input rasters', 'data and mask have different number of items but mask items is larger than 1', 'ProcessParameterInvalid')

            for i in range(len(dataRasters)):
                dRaster: RasterData = dataRasters[i]
                mRaster: RasterData = maskRasters[i]
                self._validateRasterCompatibility(toServer, job_id, dRaster, mRaster)
                resampleNeeded = not self.checkSpatialDimensions([dRaster, mRaster])
                rasters.append({"data": dRaster, "mask": mRaster, "resampleneeded": resampleNeeded})
        else:
            for i in range(len(dataRasters)):
                dRaster: RasterData = dataRasters[i]
                mRaster: RasterData = maskRasters[0]  # Single mask applies to all data rasters
                resampleNeeded = not self.checkSpatialDimensions([dRaster, mRaster])
                rasters.append({"data": dRaster, "mask": mRaster, "resampleneeded": resampleNeeded})

        return rasters
    
    def _initializePreparation(self, arguments):
        """
        Initializes the preparation process by extracting default arguments.

        Args:
            arguments: The input arguments for the operation.

        Returns:
            A tuple containing the server object and job ID.
        """
        toServer, job_id = self.getDefaultArgs(arguments)
        self.logStartPrepareOperation(job_id)
        return toServer, job_id    
        
    def _finalizePreparation(self, job_id):
        """
        Finalizes the preparation process by setting up the working catalog and marking the operation as runnable.

        Args:
            job_id: The job ID for logging.
        """
        self.createExtra(self.rasters[0]['data'])
        setWorkingCatalog(self.rasters[0]['data'], self.name)
        self.runnable = True
        self.logEndPrepareOperation(job_id)  
  
    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)

            outputRasters = []
            for item in self.rasters:
                maskMap = item["mask"]
                if item["resampleneeded"]:
                    maskMap = self.resample(item["data"], maskMap)
                  
                expression = 'iff(@1 != 0,' + str(self.replacement) + ',@2)' 
                ilwRasters = []
                idxs = []
                count = 0
                                
                for ras in self.rasters:
                    for raster in ras.getRasters(): 
                        outputRc = ilwis.do("mapcalc", expression, maskMap.getRaster(), raster) 
                        ilwRasters.append(outputRc) 
                        idxs.append(count)
                        count = count + 1

                common.registerIlwisIds(openeojob.job_id, ilwRasters)  
                outputRasters.extend(self.makeOutput(ilwRasters, self.extra))

            self.logEndOperation(processOutput,openeojob, outputs=outputRasters)
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTERLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR)
        
def registerOperation():
     return MaskOperation()
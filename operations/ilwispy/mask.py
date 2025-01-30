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
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id) 

        self.rasterSizesEqual = True                     
        dataRasters = arguments['data']['resolved'] 
        maskRasters = arguments['mask']['resolved']

        if not overlaps(dataRasters[0]['boundingbox'],maskRasters[0]['boundingbox']):
            self.handleError(toServer, job_id, 'input rasters', 'data and mask dont overlap', 'ProcessParameterInvalid')
        self.replacement = arguments['replacement']['resolved']
        self.singleMask = len(maskRasters) == 1
        self.rasters = []
        if not self.singleMask:
            if not (len( dataRasters) == len(maskRasters)):
                self.handleError(toServer, job_id, 'input rasters', 'data and mask have different number of items but mask items is larger then 1', 'ProcessParameterInvalid')
            for i in range(len(dataRasters)): 
                dRaster: RasterData = dataRasters[i]
                mRaster: RasterData = maskRasters[i]              
                if not matchBands(dRaster.getBands(), mRaster.getBands()):
                    self.handleError(toServer, job_id, 'input rasters', 'data and mask have different number of bands', 'ProcessParameterInvalid')
                if not matchesTemporalExtent(dRaster['layers'], mRaster['layers']):
                    self.handleError(toServer, job_id, 'input rasters', 'data and mask have different temporal extent', 'ProcessParameterInvalid')
                resampleNeeded = not self.checkSpatialDimensions([dRaster, mRaster])
                self.rasters.append({"data" : dRaster, "mask" : mRaster , "resampleneeded": resampleNeeded} )   
        else:
            for i in range(len(dataRasters)):
                dRaster: RasterData = dataRasters[i]
                mRaster: RasterData = maskRasters[i] 
                resampleNeeded = not self.checkSpatialDimensions([dRaster, mRaster])
                self.rasters.append({"data" : dRaster, "mask" : mRaster , "resampleneeded": resampleNeeded} )  
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

                common.registerIlwisIds(ilwRasters)  
                outputRasters.extend(self.makeOutput(ilwRasters, self.extra))

            self.logEndOperation(processOutput,openeojob, outputs=outputRasters)
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTERLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR)
        
def registerOperation():
     return MaskOperation()
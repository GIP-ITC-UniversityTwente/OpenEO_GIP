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
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']

        self.rasterSizesEqual = True                     
        dataRasters = arguments['data']['resolved'] 
        maskRasters = arguments['mask']['resolved']
        self.singleMask = len(maskRasters) == 1
        self.rasters = []
        if not self.singleMask:
            if not (len( dataRasters) == len(maskRasters)):
                self.handleError(toServer, job_id, 'input rasters', 'data and mask have different number of items but mask items is larger then 1', 'ProcessParameterInvalid')
            for i in range(len(dataRasters)): 
                dRaster: RasterData = dataRasters[i]
                mRaster: RasterData = maskRasters[i]              
                if not matchBands(dRaster.bands, mRaster.bands):
                    self.handleError(toServer, job_id, 'input rasters', 'data and mask have different number of bands', 'ProcessParameterInvalid')
                if not matchesTemporalExtent(dRaster.layers, mRaster.layers):
                    self.handleError(toServer, job_id, 'input rasters', 'data and mask have different temporal extent', 'ProcessParameterInvalid')
                resampleNeeded = not self.checkSpatialDimensions([dRaster, mRaster])
                self.rasters.append({"data" : dRaster, "mask" : mRaster , "resampleneeded": resampleNeeded} )   
        else:
            for i in range(len(dataRasters)):
                dRaster: RasterData = dataRasters[i]
                mRaster: RasterData = maskRasters[i] 
                resampleNeeded = not self.checkSpatialDimensions([dRaster, mRaster])
                self.rasters.append({"data" : dRaster, "mask" : mRaster , "resampleneeded": resampleNeeded} )                                                   
        self.runnable = True


    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)

            outputRasters = []
            for item in self.rasters:
                maskMap = item["mask"]
                if item["resampleneeded"]:
                    maskMap = self.resample(item["data"], maskMap)
                expression = 'iff(@1 != 0, @2,' + constants.RSUNDEF + ')'
                outputRc = ilwis.do("mapcalc", expression, item['mask'].getRaster().rasterImp(), item['data'].getRaster().rasterImp())                    
                outputRasters.extend(self.setOutput([outputRc], self.extra))

            self.logEndOperation(processOutput,openeojob)
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTERLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR)
        
def registerOperation():
     return MaskOperation()
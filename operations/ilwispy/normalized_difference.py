from openeooperation import *
from operationconstants import *
from constants import constants
from common import openeoip_config


class NormalizedDifference(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('normalized_difference.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']        
        self.rasterSizesEqual = True
        self.inputRaster1 = arguments['x']['resolved']
        self.inputRaster2 = arguments['y']['resolved']
        if not( isinstance(self.inputRaster2, RasterData) and isinstance(self.inputRaster1, RasterData)):
            self.handleError(toServer, job_id, 'Input raster','invalid input. rasters are not valid', 'ProcessParameterInvalid')
        
        self.createExtra(self.inputRaster1, 0) 
        self.runnable = True
              

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            outputRc = ilwis.do("mapcalc", "(@1 - @2) / (@1 + @2)", self.inputRaster1.getRaster().rasterImp(), self.inputRaster2.getRaster().rasterImp())
            outputRasters = []                
            outputRasters.extend(self.setOutput([outputRc], self.extra))
            self.logEndOperation(processOutput,openeojob)
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTERLIST)
        message = common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', message, constants.DTERROR)
        
def registerOperation():
     return NormalizedDifference()
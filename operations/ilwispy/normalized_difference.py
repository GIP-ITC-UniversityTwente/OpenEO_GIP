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
        if len(self.inputRaster1) != len(self.inputRaster2):
            self.handleError(toServer, job_id, 'Input paremeter', 'raster list must have equal length between two lists', 'ProcessParameterInvalid')

        for idx in range(len(self.inputRaster1)):           
            if not( isinstance(self.inputRaster2[idx], RasterData) and isinstance(self.inputRaster1[idx], RasterData)):
                self.handleError(toServer, job_id, 'Input raster','invalid input. rasters are not valid', 'ProcessParameterInvalid')
        
        self.createExtra(self.inputRaster1[0], 0) 
        setWorkingCatalog(self.inputRaster1[0]) 
        self.runnable = True
              

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            outputRasters = []
            ilwRasters = []
            for idx in range(len(self.inputRaster1)): 
                r1 = self.inputRaster1[idx]
                r2 = self.inputRaster2[idx]

                outputRc = ilwis.do("mapcalc", "(@1 - @2) / (@1 + @2)", r1.getRaster(), self.compatibleRaster(r1, r2).getRaster())
                ilwRasters.append(outputRc)
            common.registerIlwisIds(ilwRasters)  
            outputRasters.extend(self.setOutput(ilwRasters, self.extra))
            self.logEndOperation(processOutput,openeojob, outputs=outputRasters)
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTERLIST)
        message = common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', message, constants.DTERROR)
        
def registerOperation():
     return NormalizedDifference()